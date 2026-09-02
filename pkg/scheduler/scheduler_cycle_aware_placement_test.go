/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	"sigs.k8s.io/kueue/pkg/workload"
)

// TestScheduleCycleAwarePlacement covers the behaviour behind
// https://github.com/kubernetes-sigs/kueue/issues/14998: the simulate-empty
// reserve placement pass must see the topology capacity the same scheduling
// cycle has already committed, or contending entries converge on one domain.
func TestScheduleCycleAwarePlacement(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	defaultNodeX1 := *testingnode.MakeNode("x1").
		Label("tas-node", "true").
		Label(corev1.LabelHostname, "x1").
		StatusAllocatable(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("4"),
			corev1.ResourcePods: resource.MustParse("10"),
		}).
		Ready().
		Obj()
	defaultNodeY1 := *testingnode.MakeNode("y1").
		Label("tas-node", "true").
		Label(corev1.LabelHostname, "y1").
		StatusAllocatable(corev1.ResourceList{
			corev1.ResourceCPU:  resource.MustParse("4"),
			corev1.ResourcePods: resource.MustParse("10"),
		}).
		Ready().
		Obj()
	defaultSingleLevelTopology := *utiltestingapi.MakeDefaultOneLevelTopology("tas-single-level")
	defaultTASFlavor := *utiltestingapi.MakeResourceFlavor("tas-default").
		NodeLabel("tas-node", "true").
		TopologyName("tas-single-level").
		Obj()
	// No preemption configured anywhere: the pending gangs find no preemption
	// candidates, and with ReclaimWithinCohort unset (Never) the
	// no-candidates reserve path fires, committing each gang's reserved
	// topology domain within the cycle.
	defaultClusterQueueA := *utiltestingapi.MakeClusterQueue("tas-cq-a").
		Cohort("tas-cohort-main").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("tas-default").
			Resource(corev1.ResourceCPU, "6").Obj()).
		Obj()
	defaultClusterQueueB := *utiltestingapi.MakeClusterQueue("tas-cq-b").
		Cohort("tas-cohort-main").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("tas-default").
			Resource(corev1.ResourceCPU, "4").Obj()).
		Obj()
	defaultClusterQueueC := *utiltestingapi.MakeClusterQueue("tas-cq-c").
		Cohort("tas-cohort-main").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("tas-default").
			Resource(corev1.ResourceCPU, "4").Obj()).
		Obj()
	queues := []kueue.LocalQueue{
		*utiltestingapi.MakeLocalQueue("tas-lq-a", "default").ClusterQueue("tas-cq-a").Obj(),
		*utiltestingapi.MakeLocalQueue("tas-lq-b", "default").ClusterQueue("tas-cq-b").Obj(),
		*utiltestingapi.MakeLocalQueue("tas-lq-c", "default").ClusterQueue("tas-cq-c").Obj(),
	}
	eventIgnoreMessage := cmpopts.IgnoreFields(utiltesting.EventRecord{}, "Message")
	cases := map[string]tasScheduleTestCase{
		// Nodes x1 and y1 hold exactly one gang (4 CPU) each; y1 already hosts
		// 1 CPU of admitted work, so x1 is the only domain a whole gang fits
		// in. Quota fits for everyone, so topology is the only constraint.
		//
		// w1 takes x1 and commits it. w2 can no longer fit and reserves the
		// only vacatable domain: y1. w3, a single pod, can go wherever a CPU
		// is left — but once w2 has reserved y1 there is none.
		//
		// Before the fix, w2's simulate-empty reserve search could not see
		// w1's just-committed x1, so it reserved x1 a second time instead of
		// y1; the misplacement then left y1 looking free and w3 was wrongly
		// admitted into it. Now w2 reserves y1 and w3 correctly stays queued.
		"two ClusterQueues reserving in one cycle take distinct topology domains": {
			featureGates: map[featuregate.Feature]bool{
				features.TASRecomputeAssignmentWithinSchedulingCycle: true,
			},
			nodes:           []corev1.Node{defaultNodeX1, defaultNodeY1},
			topologies:      []kueue.Topology{defaultSingleLevelTopology},
			resourceFlavors: []kueue.ResourceFlavor{defaultTASFlavor},
			clusterQueues:   []kueue.ClusterQueue{defaultClusterQueueA, defaultClusterQueueB, defaultClusterQueueC},
			workloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("a0", "default").
					Queue("tas-lq-a").
					Priority(3).
					ReserveQuotaAt(
						utiltestingapi.MakeAdmission("tas-cq-a").
							PodSets(utiltestingapi.MakePodSetAssignment("one").
								Assignment(corev1.ResourceCPU, "tas-default", "1").
								TopologyAssignment(utiltestingapi.MakeTopologyAssignment(utiltas.Levels(&defaultSingleLevelTopology)).
									Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"y1"}, 1).Obj()).
									Obj()).
								Obj()).
							Obj(),
						now,
					).
					AdmittedAt(true, now).
					PodSets(*utiltestingapi.MakePodSet("one", 1).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Obj(),
				*utiltestingapi.MakeWorkload("w1", "default").
					UID("wl-w1").
					JobUID("job-w1").
					Queue("tas-lq-a").
					Creation(now.Add(-time.Minute)).
					Priority(10).
					PodSets(*utiltestingapi.MakePodSet("one", 4).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Obj(),
				*utiltestingapi.MakeWorkload("w2", "default").
					UID("wl-w2").
					JobUID("job-w2").
					Queue("tas-lq-b").
					Creation(now).
					Priority(5).
					PodSets(*utiltestingapi.MakePodSet("one", 4).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Obj(),
				*utiltestingapi.MakeWorkload("w3", "default").
					UID("wl-w3").
					JobUID("job-w3").
					Queue("tas-lq-c").
					Creation(now).
					Priority(4).
					PodSets(*utiltestingapi.MakePodSet("one", 1).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Obj(),
			},
			wantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload("a0", "default").
					Queue("tas-lq-a").
					Priority(3).
					ReserveQuotaAt(
						utiltestingapi.MakeAdmission("tas-cq-a").
							PodSets(utiltestingapi.MakePodSetAssignment("one").
								Assignment(corev1.ResourceCPU, "tas-default", "1").
								TopologyAssignment(utiltestingapi.MakeTopologyAssignment(utiltas.Levels(&defaultSingleLevelTopology)).
									Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"y1"}, 1).Obj()).
									Obj()).
								Obj()).
							Obj(),
						now,
					).
					AdmittedAt(true, now).
					PodSets(*utiltestingapi.MakePodSet("one", 1).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Obj(),
				*utiltestingapi.MakeWorkload("w1", "default").
					UID("wl-w1").
					JobUID("job-w1").
					Queue("tas-lq-a").
					Creation(now.Add(-time.Minute)).
					Priority(10).
					PodSets(*utiltestingapi.MakePodSet("one", 4).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadQuotaReserved,
						Status:             metav1.ConditionTrue,
						Reason:             "QuotaReserved",
						Message:            "Quota reserved in ClusterQueue tas-cq-a",
						LastTransitionTime: metav1.NewTime(now),
					}).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadAdmitted,
						Status:             metav1.ConditionTrue,
						Reason:             "Admitted",
						Message:            "The workload is admitted",
						LastTransitionTime: metav1.NewTime(now),
					}).
					Admission(utiltestingapi.MakeAdmission("tas-cq-a").
						PodSets(utiltestingapi.MakePodSetAssignment("one").
							Count(4).
							Assignment(corev1.ResourceCPU, "tas-default", "4").
							TopologyAssignment(utiltestingapi.MakeTopologyAssignment(utiltas.Levels(&defaultSingleLevelTopology)).
								Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"x1"}, 4).Obj()).
								Obj()).
							Obj()).
						Obj()).
					Obj(),
				*utiltestingapi.MakeWorkload("w2", "default").
					UID("wl-w2").
					JobUID("job-w2").
					Queue("tas-lq-b").
					Creation(now).
					Priority(5).
					PodSets(*utiltestingapi.MakePodSet("one", 4).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadQuotaReserved,
						Status:             metav1.ConditionFalse,
						Reason:             kueue.WorkloadQuotaReservedReasonWaitingForQuota,
						Message:            `couldn't assign flavors to pod set one: topology "tas-single-level" allows to fit only 3 out of 4 pod(s). Total nodes: 2; excluded: resource "cpu": 1`,
						LastTransitionTime: metav1.NewTime(now),
					}).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadAdmitted,
						Status:             metav1.ConditionFalse,
						Reason:             kueue.WorkloadAdmittedReasonNoReservation,
						Message:            "The workload has no reservation",
						LastTransitionTime: metav1.NewTime(now),
					}).
					ResourceRequests(kueue.PodSetRequest{
						Name: "one",
						Resources: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("4"),
						},
					}).
					Obj(),
				*utiltestingapi.MakeWorkload("w3", "default").
					UID("wl-w3").
					JobUID("job-w3").
					Queue("tas-lq-c").
					Creation(now).
					Priority(4).
					PodSets(*utiltestingapi.MakePodSet("one", 1).
						RequiredTopologyRequest(corev1.LabelHostname).
						Request(corev1.ResourceCPU, "1").
						Obj()).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadQuotaReserved,
						Status:             metav1.ConditionFalse,
						Reason:             kueue.WorkloadQuotaReservedReasonTopologyPlacementFailed,
						Message:            `couldn't assign flavors to pod set one: topology "tas-single-level" doesn't allow to fit any of 1 pod(s). Total nodes: 2; excluded: resource "cpu": 2`,
						LastTransitionTime: metav1.NewTime(now),
					}).
					Condition(metav1.Condition{
						Type:               kueue.WorkloadAdmitted,
						Status:             metav1.ConditionFalse,
						Reason:             kueue.WorkloadAdmittedReasonNoReservation,
						Message:            "The workload has no reservation",
						LastTransitionTime: metav1.NewTime(now),
					}).
					ResourceRequests(kueue.PodSetRequest{
						Name: "one",
						Resources: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
						},
					}).
					Obj(),
			},
			wantNewAssignments: map[workload.Reference]kueue.Admission{
				"default/w1": *utiltestingapi.MakeAdmission("tas-cq-a").
					PodSets(utiltestingapi.MakePodSetAssignment("one").
						Assignment(corev1.ResourceCPU, "tas-default", "4").
						Count(4).
						TopologyAssignment(utiltestingapi.MakeTopologyAssignment(utiltas.Levels(&defaultSingleLevelTopology)).
							Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"x1"}, 4).Obj()).
							Obj()).
						Obj()).
					Obj(),
			},
			wantInadmissibleLeft: map[kueue.ClusterQueueReference][]workload.Reference{
				"tas-cq-b": {"default/w2"},
				"tas-cq-c": {"default/w3"},
			},
			eventCmpOpts: cmp.Options{eventIgnoreMessage},
			wantEvents: []utiltesting.EventRecord{
				utiltesting.MakeEventRecord("default", "w1", "QuotaReserved", corev1.EventTypeNormal).Obj(),
				utiltesting.MakeEventRecord("default", "w1", "Admitted", corev1.EventTypeNormal).Obj(),
				utiltesting.MakeEventRecord("default", "w2", kueue.WorkloadQuotaReservedReasonWaitingForQuota, corev1.EventTypeWarning).Obj(),
				utiltesting.MakeEventRecord("default", "w3", kueue.WorkloadQuotaReservedReasonTopologyPlacementFailed, corev1.EventTypeWarning).Obj(),
			},
		},
	}
	runTASScheduleTestCases(t, tasScheduleTestConfig{
		queues: queues,
		now:    now,
	}, cases)
}
