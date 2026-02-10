/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link ExecutionDeployer}. */
public class DefaultExecutionDeployer implements ExecutionDeployer {

    // -------------------------------- FALCON Implementation --------------------------------
    /**
     * When successfully assigning all the subTasks to physical slots, initialize falcon heap memory management system.
     * <p> First, calculate the number of subTasks in each slot/{@link AllocationID}. Then, add this info to each
     * subTask/{@link ExecutionAttemptID}.
     * <p> Note that {@link ExecutionAttemptID} will be used to create tdd for subTask, and tdd can be accessed in
     * TaskManager. In this way, for each subTask, we can get "the number for subTasks sharing the same slot" when
     * initializing StreamTask in TaskManager.
     * @brief FALCON implementation
     * @param deploymentHandles handles used to deploy subTasks to physical slots
     */
    void InitFalconMemoryManager(final List<ExecutionDeploymentHandle> deploymentHandles) {
        // if enableFalconCache, initialize falcon heap memory manager, and then deploy subtasks to slots
        final boolean enableFalconCache = GlobalConfiguration.loadConfiguration().get(
                ConfigOptions.key("state.backend.rocksdb.falcon.use-state-cache")
                        .booleanType()
                        .defaultValue(false)
                        .withDescription("If true, falcon cache will be used for RocksDBValueState/RocksDBMapState.")
        );

        if (enableFalconCache) {
            // this log will be print only once in JobManager
            log.info("[FALCON] initializing falcon cache heap memory management system.");
            // step1: get the number of subTasks in each physical slot
            Map<AllocationID, Integer> numSubTaskInSlot = new HashMap<>();
            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                // get allocationId of current subTask from deploy handle
                final CompletableFuture<LogicalSlot> slotAssigned = deploymentHandle.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());
                AllocationID allocationId = slotAssigned.join().getAllocationId();
                // update mapping from slot to the number of subTasks in slot
                if (numSubTaskInSlot.containsKey(allocationId)) {
                    int currentCnt = numSubTaskInSlot.get(allocationId);
                    numSubTaskInSlot.put(allocationId, currentCnt + 1);
                } else {
                    numSubTaskInSlot.put(allocationId, 1);
                }
            }

            // step2: add "the number for subTasks sharing the same slot" to each subTask/ExecutionAttemptID
            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                ExecutionVertexID executionVertexId = deploymentHandle.getRequiredVertexVersion().getExecutionVertexId();
                AllocationID allocationId = deploymentHandle.getLogicalSlotFuture().join().getAllocationId();
                int subTaskNum = numSubTaskInSlot.get(allocationId);

                deploymentHandle.getExecution().getAttemptId().setSubTaskNum(subTaskNum);
                if (deploymentHandles.size() <= 32) { // avoid too much log printing
                    log.info("[FALCON] subTask {}: slot {} has {} subTasks", executionVertexId.toString(), allocationId,
                            subTaskNum);
                }
            }
        }
    }
    // ---------------------------------------------------------------------------------------

    private final Logger log;

    private final ExecutionSlotAllocator executionSlotAllocator;

    private final ExecutionOperations executionOperations;

    private final ExecutionVertexVersioner executionVertexVersioner;

    private final Time partitionRegistrationTimeout;

    private final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    private DefaultExecutionDeployer(
            final Logger log,
            final ExecutionSlotAllocator executionSlotAllocator,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final Time partitionRegistrationTimeout,
            final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
            final ComponentMainThreadExecutor mainThreadExecutor) {

        this.log = checkNotNull(log);
        this.executionSlotAllocator = checkNotNull(executionSlotAllocator);
        this.executionOperations = checkNotNull(executionOperations);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.partitionRegistrationTimeout = checkNotNull(partitionRegistrationTimeout);
        this.allocationReservationFunc = checkNotNull(allocationReservationFunc);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
    }

    @Override
    public void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex) {
        validateExecutionStates(executionsToDeploy);

        transitionToScheduled(executionsToDeploy);

        final List<ExecutionSlotAssignment> executionSlotAssignments =
                allocateSlotsFor(executionsToDeploy);

        final List<ExecutionDeploymentHandle> deploymentHandles =
                createDeploymentHandles(
                        executionsToDeploy, requiredVersionByVertex, executionSlotAssignments);

        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateExecutionStates(final Collection<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(
                e ->
                        checkState(
                                e.getState() == ExecutionState.CREATED,
                                "Expected execution %s to be in CREATED state, was: %s",
                                e.getAttemptId(),
                                e.getState()));
    }

    private void transitionToScheduled(final List<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(e -> e.transitionState(ExecutionState.SCHEDULED));
    }

    private List<ExecutionSlotAssignment> allocateSlotsFor(
            final List<Execution> executionsToDeploy) {
        final List<ExecutionAttemptID> executionAttemptIds =
                executionsToDeploy.stream()
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toList());
        return executionSlotAllocator.allocateSlotsFor(executionAttemptIds);
    }

    private List<ExecutionDeploymentHandle> createDeploymentHandles(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final List<ExecutionSlotAssignment> executionSlotAssignments) {

        final List<ExecutionDeploymentHandle> deploymentHandles =
                new ArrayList<>(executionsToDeploy.size());
        for (int i = 0; i < executionsToDeploy.size(); i++) {
            final Execution execution = executionsToDeploy.get(i);
            final ExecutionSlotAssignment assignment = executionSlotAssignments.get(i);
            checkState(execution.getAttemptId().equals(assignment.getExecutionAttemptId()));

            final ExecutionVertexID executionVertexId = execution.getVertex().getID();
            final ExecutionDeploymentHandle deploymentHandle =
                    new ExecutionDeploymentHandle(
                            execution, assignment, requiredVersionByVertex.get(executionVertexId));
            deploymentHandles.add(deploymentHandle);
        }

        return deploymentHandles;
    }

    private void waitForAllSlotsAndDeploy(final List<ExecutionDeploymentHandle> deploymentHandles) {
        FutureUtils.assertNoException(
                assignAllResourcesAndRegisterProducedPartitions(deploymentHandles)
                        .handle(deployAll(deploymentHandles)));
    }

    private CompletableFuture<Void> assignAllResourcesAndRegisterProducedPartitions(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> resultFutures = new ArrayList<>();
        for (ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> resultFuture =
                    deploymentHandle
                            .getLogicalSlotFuture()
                            .handle(assignResource(deploymentHandle))
                            .thenCompose(registerProducedPartitions(deploymentHandle))
                            .handle(
                                    (ignore, throwable) -> {
                                        if (throwable != null) {
                                            handleTaskDeploymentFailure(
                                                    deploymentHandle.getExecution(), throwable);
                                        }
                                        return null;
                                    });

            resultFutures.add(resultFuture);
        }
        return FutureUtils.waitForAll(resultFutures);
    }

    private BiFunction<Void, Throwable, Void> deployAll(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);

            InitFalconMemoryManager(deploymentHandles); // [FALCON] calculate subTask number in each slot

            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                final CompletableFuture<LogicalSlot> slotAssigned =
                        deploymentHandle.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());

                FutureUtils.assertNoException(
                        slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    private BiFunction<LogicalSlot, Throwable, LogicalSlot> assignResource(
            final ExecutionDeploymentHandle deploymentHandle) {

        return (logicalSlot, throwable) -> {
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                    releaseSlotIfPresent(logicalSlot);
                }
                return null;
            }

            // throw exception only if the execution version is not outdated.
            // this ensures that canceling a pending slot request does not fail
            // a task which is about to cancel.
            if (throwable != null) {
                throw new CompletionException(maybeWrapWithNoResourceAvailableException(throwable));
            }

            if (!execution.tryAssignResource(logicalSlot)) {
                throw new IllegalStateException(
                        "Could not assign resource "
                                + logicalSlot
                                + " to execution "
                                + execution
                                + '.');
            }

            // We only reserve the latest execution of an execution vertex. Because it may cause
            // problems to reserve multiple slots for one execution vertex. Besides that, slot
            // reservation is for local recovery and therefore is only needed by streaming jobs, in
            // which case an execution vertex will have one only current execution.
            allocationReservationFunc.accept(
                    execution.getAttemptId().getExecutionVertexId(), logicalSlot.getAllocationId());

            return logicalSlot;
        };
    }

    private static void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. "
                            + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    private Function<LogicalSlot, CompletableFuture<Void>> registerProducedPartitions(
            final ExecutionDeploymentHandle deploymentHandle) {

        return logicalSlot -> {
            // a null logicalSlot means the slot assignment is skipped, in which case
            // the produced partition registration process can be skipped as well
            if (logicalSlot != null) {
                final Execution execution = deploymentHandle.getExecution();
                final CompletableFuture<Void> partitionRegistrationFuture =
                        execution.registerProducedPartitions(logicalSlot.getTaskManagerLocation());

                return FutureUtils.orTimeout(
                        partitionRegistrationFuture,
                        partitionRegistrationTimeout.toMilliseconds(),
                        TimeUnit.MILLISECONDS,
                        mainThreadExecutor);
            } else {
                return FutureUtils.completedVoidFuture();
            }
        };
    }

    private BiFunction<Object, Throwable, Void> deployOrHandleError(
            final ExecutionDeploymentHandle deploymentHandle) {

        return (ignored, throwable) -> {
            final ExecutionVertexVersion requiredVertexVersion =
                    deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED
                    || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                }
                return null;
            }

            if (throwable == null) {
                deployTaskSafe(execution);
            } else {
                handleTaskDeploymentFailure(execution, throwable);
            }
            return null;
        };
    }

    private void deployTaskSafe(final Execution execution) {
        try {
            executionOperations.deploy(execution);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(execution, e);
        }
    }

    private void handleTaskDeploymentFailure(final Execution execution, final Throwable error) {
        executionOperations.markFailed(execution, error);
    }

    private static class ExecutionDeploymentHandle {

        private final Execution execution;

        private final ExecutionSlotAssignment executionSlotAssignment;

        private final ExecutionVertexVersion requiredVertexVersion;

        ExecutionDeploymentHandle(
                final Execution execution,
                final ExecutionSlotAssignment executionSlotAssignment,
                final ExecutionVertexVersion requiredVertexVersion) {
            this.execution = checkNotNull(execution);
            this.executionSlotAssignment = checkNotNull(executionSlotAssignment);
            this.requiredVertexVersion = checkNotNull(requiredVertexVersion);
        }

        Execution getExecution() {
            return execution;
        }

        ExecutionAttemptID getExecutionAttemptId() {
            return execution.getAttemptId();
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return executionSlotAssignment.getLogicalSlotFuture();
        }

        ExecutionVertexVersion getRequiredVertexVersion() {
            return requiredVertexVersion;
        }
    }

    /** Factory to instantiate the {@link DefaultExecutionDeployer}. */
    public static class Factory implements ExecutionDeployer.Factory {

        @Override
        public DefaultExecutionDeployer createInstance(
                Logger log,
                ExecutionSlotAllocator executionSlotAllocator,
                ExecutionOperations executionOperations,
                ExecutionVertexVersioner executionVertexVersioner,
                Time partitionRegistrationTimeout,
                BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                ComponentMainThreadExecutor mainThreadExecutor) {
            return new DefaultExecutionDeployer(
                    log,
                    executionSlotAllocator,
                    executionOperations,
                    executionVertexVersioner,
                    partitionRegistrationTimeout,
                    allocationReservationFunc,
                    mainThreadExecutor);
        }
    }
}
