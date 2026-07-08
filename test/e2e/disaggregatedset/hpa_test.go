/*
Copyright 2025 The Kubernetes Authors.

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

package e2e

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	utils "sigs.k8s.io/lws/test/testutils/disaggregatedset"
	"sigs.k8s.io/lws/test/testutils/disaggregatedset/fixtures"
	"sigs.k8s.io/lws/test/testutils/disaggregatedset/kubectl"
)

// hpaDSName is the DisaggregatedSet used by the HPA e2e suite. The
// auto-created scaler for the prefill role therefore lives at
// "<hpaDSName>-prefill" — the DS controller uses this deterministic name.
const hpaDSName = "hpa-e2e"

// autoScalerName is the deterministic scaler name that the DS controller
// creates for the prefill role.
func autoScalerName(role string) string {
	return fmt.Sprintf("%s-%s", hpaDSName, role)
}

func getScalerObservedReplicas(name string) string {
	out, _ := kubectl.Get("disaggregatedsetrolescaler", name).
		Namespace("default").
		JSONPath("{.status.replicas}").
		RunQuiet()
	return strings.TrimSpace(out)
}

func getScalerSelector(name string) string {
	out, _ := kubectl.Get("disaggregatedsetrolescaler", name).
		Namespace("default").
		JSONPath("{.status.selector}").
		RunQuiet()
	return strings.TrimSpace(out)
}

func getScalerSpecReplicas(name string) string {
	out, _ := kubectl.Get("disaggregatedsetrolescaler", name).
		Namespace("default").
		JSONPath("{.spec.replicas}").
		RunQuiet()
	return strings.TrimSpace(out)
}

func hpaLWSReplicas(role string) int {
	out, err := kubectl.LWSByRole(hpaDSName, role).
		JSONPath("{.items[0].spec.replicas}").
		RunQuiet()
	Expect(err).NotTo(HaveOccurred())
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0
	}
	return n
}

func getDSCondition(deploymentName, condType string) string {
	out, _ := kubectl.Get("disaggregatedset", deploymentName).
		Namespace("default").
		JSONPath(`{range .status.conditions[?(@.type=="` + condType + `")]}{.status}{end}`).
		RunQuiet()
	return strings.TrimSpace(out)
}

func scaleScaler(name string, replicas int) error {
	cmd := exec.Command("kubectl", "scale", "disaggregatedsetrolescaler", name,
		"--replicas", strconv.Itoa(replicas), "-n", "default")
	_, err := utils.Run(cmd)
	return err
}

var _ = Describe("DisaggregatedSet HPA integration — autocreate (KEP-849)", Ordered, func() {
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	const dsName = hpaDSName
	scalerName := autoScalerName("prefill")

	AfterAll(func() {
		By("cleaning up the DisaggregatedSet (cascades to the auto-created scaler)")
		kubectl.CleanupDeployment(dsName)
		_, _ = kubectl.Delete("disaggregatedsetrolescaler", scalerName).
			Namespace("default").IgnoreNotFound().RunQuiet()
	})

	It("auto-creates a scaler for an External role and seeds it from initialReplicas", func() {
		By("applying a DisaggregatedSet with an External prefill role (initialReplicas: 2) + Static decode role")
		yaml := fixtures.Config{
			Name:      dsName,
			Namespace: "default",
			Roles: []fixtures.Role{
				{Name: "prefill", External: true, InitialReplicas: fixtures.Ptr(2)},
				{Name: "decode", Replicas: 1},
			},
		}.YAML()
		Expect(applyYAML(yaml)).To(Succeed())

		By("verifying the scaler was auto-created at the deterministic name")
		Eventually(func(g Gomega) {
			out, err := kubectl.Get("disaggregatedsetrolescaler", scalerName).
				Namespace("default").
				JSONPath("{.metadata.name}").
				RunQuiet()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal(scalerName))
		}).Should(Succeed())

		By("verifying the auto-created scaler is controller-owned by the DS")
		out, err := kubectl.Get("disaggregatedsetrolescaler", scalerName).
			Namespace("default").
			JSONPath("{.metadata.ownerReferences[0].controller}").
			RunQuiet()
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(out)).To(Equal("true"))

		By("verifying the scaler seed value made the prefill LWS scale to 2")
		Eventually(func(g Gomega) {
			g.Expect(getScalerSpecReplicas(scalerName)).To(Equal("2"))
			g.Expect(hpaLWSReplicas("prefill")).To(Equal(2))
		}).Should(Succeed())

		By("verifying the decode LWS is created and scales to 1")
		Eventually(func(g Gomega) {
			g.Expect(hpaLWSReplicas("decode")).To(Equal(1))
		}).Should(Succeed())

		By("verifying WaitingForScaler is False (scaler exists and has replicas)")
		Eventually(func(g Gomega) {
			g.Expect(getDSCondition(dsName, "WaitingForScaler")).To(Equal("False"))
		}).Should(Succeed())
	})

	It("scales the LWS when /scale is written on the scaler (simulates HPA)", func() {
		By("writing replicas=4 via the /scale subresource")
		Expect(scaleScaler(scalerName, 4)).To(Succeed())

		By("verifying the LWS scales to 4")
		Eventually(func(g Gomega) {
			g.Expect(hpaLWSReplicas("prefill")).To(Equal(4))
		}).Should(Succeed())

		By("verifying scaler.status.replicas reflects the new observed count")
		Eventually(func(g Gomega) {
			g.Expect(getScalerObservedReplicas(scalerName)).To(Equal("4"))
			g.Expect(getScalerSelector(scalerName)).To(ContainSubstring("leaderworkerset.sigs.k8s.io/name="))
		}).Should(Succeed())
	})

	It("recreates the scaler if the user deletes it, and holds the LWS at its current count", func() {
		By("deleting the auto-created scaler")
		_, err := kubectl.Delete("disaggregatedsetrolescaler", scalerName).
			Namespace("default").
			RunQuiet()
		Expect(err).NotTo(HaveOccurred())

		By("verifying the controller recreates the scaler")
		Eventually(func(g Gomega) {
			out, err := kubectl.Get("disaggregatedsetrolescaler", scalerName).
				Namespace("default").
				JSONPath("{.metadata.name}").
				RunQuiet()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal(scalerName))
		}).Should(Succeed())

		By("verifying the LWS holds at 4 (WaitingForScaler until HPA writes again)")
		Consistently(func(g Gomega) {
			g.Expect(hpaLWSReplicas("prefill")).To(Equal(4))
		}, 15*time.Second, time.Second).Should(Succeed())
	})

	It("garbage-collects the scaler when the DisaggregatedSet is deleted", func() {
		By("deleting the DisaggregatedSet")
		_, err := kubectl.Delete("disaggregatedset", dsName).
			Namespace("default").
			RunQuiet()
		Expect(err).NotTo(HaveOccurred())

		By("verifying the scaler is garbage-collected via the controller ownerRef")
		Eventually(func(g Gomega) {
			out, _ := kubectl.Get("disaggregatedsetrolescaler", scalerName).
				Namespace("default").
				IgnoreNotFound().
				RunQuiet()
			g.Expect(strings.TrimSpace(out)).To(BeEmpty())
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
	})
})
