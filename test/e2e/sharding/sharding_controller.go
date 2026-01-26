/*
Copyright 2025 The Volcano Authors.

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

package sharding

import (
	"context"
	"fmt"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	shardv1alpha1 "volcano.sh/apis/pkg/apis/shard/v1alpha1"
	e2eutil "volcano.sh/volcano/test/e2e/util"
)

var _ = Describe("Sharding Controller E2E Test", func() {
	var ctx *e2eutil.TestContext

	BeforeEach(func() {
		ctx = e2eutil.InitTestContext(e2eutil.Options{})
	})

	AfterEach(func() {
		if ctx != nil {
			e2eutil.CleanupTestContext(ctx)
		}
	})

	It("Basic Shard Creation and Initialization", func() {
		By("Getting cluster nodes")
		nodes, err := ctx.Kubeclient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(len(nodes.Items)).To(BeNumerically(">", 1), "Cluster should have at least two nodes")

		var lowUtilNode, highUtilNode *v1.Node
		lowUtilNode = &nodes.Items[0]
		highUtilNode = &nodes.Items[1]

		By("Setting low CPU utilization on first node (< 60%)")
		cpuCapacity := lowUtilNode.Status.Capacity.Cpu().MilliValue()
		lowUtilReq := int64(float64(cpuCapacity) * 0.3)
		
		pod1 := e2eutil.CreatePod(ctx, e2eutil.PodSpec{
			Name: "low-util-pod",
			Node: lowUtilNode.Name,
			Req:  e2eutil.CPUResource(fmt.Sprintf("%dm", lowUtilReq)),
		})
		defer e2eutil.DeletePod(ctx, pod1)

		By("Setting high CPU utilization on second node (> 70%)")
		highUtilReq := int64(float64(highUtilNode.Status.Capacity.Cpu().MilliValue()) * 0.8)
		
		pod2 := e2eutil.CreatePod(ctx, e2eutil.PodSpec{
			Name: "high-util-pod",
			Node: highUtilNode.Name,
			Req:  e2eutil.CPUResource(fmt.Sprintf("%dm", highUtilReq)),
		})
		defer e2eutil.DeletePod(ctx, pod2)

		By("Waiting for pods to be running")
		err = e2eutil.WaitPodReady(ctx, pod1)
		Expect(err).NotTo(HaveOccurred())
		err = e2eutil.WaitPodReady(ctx, pod2)
		Expect(err).NotTo(HaveOccurred())

		By("Waiting for shard synchronization")
		Eventually(func() error {
			shards, err := ctx.Vcclient.ShardV1alpha1().NodeShards().List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				return err
			}

			shardMap := make(map[string]*shardv1alpha1.NodeShard)
			for i := range shards.Items {
				shard := &shards.Items[i]
				shardMap[shard.Name] = shard
			}

			if volcanoShard, ok := shardMap["volcano"]; !ok {
				return fmt.Errorf("volcano shard not found yet")
			} else if !slices.Contains(volcanoShard.Spec.NodesDesired, lowUtilNode.Name) {
				return fmt.Errorf("node %s not found in volcano shard", lowUtilNode.Name)
			}

			if agentShard, ok := shardMap["agent-scheduler"]; !ok {
				return fmt.Errorf("agent-scheduler shard not found yet")
			} else if !slices.Contains(agentShard.Spec.NodesDesired, highUtilNode.Name) {
				return fmt.Errorf("node %s not found in agent-scheduler shard", highUtilNode.Name)
			}

			return nil
		}, 60*time.Second, 2*time.Second).Should(Succeed())
	})
})
