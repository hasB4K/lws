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

// hpaDSName is the DisaggregatedSet used by the HPA e2e suite.
const hpaDSName = "hpa-e2e"

// getScalerObservedReplicas returns status.replicas of the scaler as a string
// ("" when the status field has not been written yet).
func getScalerObservedReplicas(name string) string {
	out, _ := kubectl.Get("disaggregatedsetrolescaler", name).
		Namespace("default").
		JSONPath("{.status.replicas}").
		RunQuiet()
	return strings.TrimSpace(out)
}

// getScalerSelector returns status.selector of the scaler.
func getScalerSelector(name string) string {
	out, _ := kubectl.Get("disaggregatedsetrolescaler", name).
		Namespace("default").
		JSONPath("{.status.selector}").
		RunQuiet()
	return strings.TrimSpace(out)
}

// hpaLWSReplicas returns spec.replicas of the (single) LWS for the HPA test's
// DisaggregatedSet + role. Fails via Gomega if no LWS is found.
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

// getDSCondition returns "" if condition is missing, else its status
// ("True", "False", "Unknown").
func getDSCondition(deploymentName, condType string) string {
	out, _ := kubectl.Get("disaggregatedset", deploymentName).
		Namespace("default").
		JSONPath(`{range .status.conditions[?(@.type=="` + condType + `")]}{.status}{end}`).
		RunQuiet()
	return strings.TrimSpace(out)
}

// scaleScaler writes spec.replicas via the /scale subresource — mirrors what
// an HPA / KEDA controller does.
func scaleScaler(name string, replicas int) error {
	cmd := exec.Command("kubectl", "scale", "disaggregatedsetrolescaler", name,
		"--replicas", strconv.Itoa(replicas), "-n", "default")
	_, err := utils.Run(cmd)
	return err
}

var _ = Describe("DisaggregatedSet HPA integration (KEP-849)", Ordered, func() {
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	const dsName = hpaDSName
	const scalerName = "hpa-e2e-prefill-scaler"

	AfterAll(func() {
		By("cleaning up any DisaggregatedSetRoleScaler left over")
		_, _ = kubectl.Delete("disaggregatedsetrolescaler", scalerName).
			Namespace("default").IgnoreNotFound().RunQuiet()

		By("cleaning up the DisaggregatedSet")
		kubectl.CleanupDeployment(dsName)
	})

	It("holds External role at 0 replicas and sets WaitingForScaler when no scaler exists", func() {
		By("applying a DisaggregatedSet with an External prefill role and a Static decode role")
		yaml := fixtures.Config{
			Name:      dsName,
			Namespace: "default",
			Roles: []fixtures.Role{
				{Name: "prefill", External: true},
				{Name: "decode", Replicas: 1},
			},
		}.YAML()
		Expect(applyYAML(yaml)).To(Succeed())

		By("verifying the prefill LWS is created at 0 replicas")
		Eventually(func(g Gomega) {
			g.Expect(kubectl.CountLWSByRole(dsName, "prefill")).To(Equal(1))
			g.Expect(hpaLWSReplicas("prefill")).To(Equal(0))
		}).Should(Succeed())

		By("verifying the decode LWS is created and scales to 1")
		Eventually(func(g Gomega) {
			g.Expect(hpaLWSReplicas("decode")).To(Equal(1))
		}).Should(Succeed())

		By("verifying DisaggregatedSet reports WaitingForScaler=True")
		Eventually(func(g Gomega) {
			g.Expect(getDSCondition(dsName, "WaitingForScaler")).To(Equal("True"))
		}).Should(Succeed())
	})

	It("scales the prefill LWS to spec.replicas of the scaler when the scaler is created", func() {
		By("creating a DisaggregatedSetRoleScaler with replicas: 2")
		scalerYAML := fixtures.ScalerConfig{
			Name:       scalerName,
			Namespace:  "default",
			TargetName: dsName,
			TargetRole: "prefill",
			Replicas:   fixtures.Ptr(2),
		}.YAML()
		Expect(applyYAML(scalerYAML)).To(Succeed())

		By("verifying the prefill LWS scales to 2")
		Eventually(func(g Gomega) {
			g.Expect(hpaLWSReplicas("prefill")).To(Equal(2))
		}).Should(Succeed())

		By("verifying WaitingForScaler flips to False")
		Eventually(func(g Gomega) {
			g.Expect(getDSCondition(dsName, "WaitingForScaler")).To(Equal("False"))
		}).Should(Succeed())

		By("verifying the scaler status.replicas mirrors the observed LWS replicas")
		Eventually(func(g Gomega) {
			g.Expect(getScalerObservedReplicas(scalerName)).To(Equal("2"))
			g.Expect(getScalerSelector(scalerName)).To(ContainSubstring("leaderworkerset.sigs.k8s.io/name="))
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
		}).Should(Succeed())
	})

	It("holds the LWS at its last replica count when the scaler is deleted mid-run", func() {
		By("deleting the scaler")
		_, err := kubectl.Delete("disaggregatedsetrolescaler", scalerName).
			Namespace("default").
			RunQuiet()
		Expect(err).NotTo(HaveOccurred())

		By("verifying DisaggregatedSet flips to WaitingForScaler=True but LWS holds at 4")
		Eventually(func(g Gomega) {
			g.Expect(getDSCondition(dsName, "WaitingForScaler")).To(Equal("True"))
			g.Expect(hpaLWSReplicas("prefill")).To(Equal(4))
		}).Should(Succeed())
	})

	It("garbage-collects the scaler when the DisaggregatedSet is deleted", func() {
		By("recreating the scaler so we can observe GC")
		scalerYAML := fixtures.ScalerConfig{
			Name:       scalerName,
			Namespace:  "default",
			TargetName: dsName,
			TargetRole: "prefill",
			Replicas:   fixtures.Ptr(1),
		}.YAML()
		Expect(applyYAML(scalerYAML)).To(Succeed())

		By("waiting for the scaler to receive its owner reference")
		Eventually(func(g Gomega) {
			out, err := kubectl.Get("disaggregatedsetrolescaler", scalerName).
				Namespace("default").
				JSONPath("{.metadata.ownerReferences[0].kind}").
				RunQuiet()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(strings.TrimSpace(out)).To(Equal("DisaggregatedSet"))
		}).Should(Succeed())

		By("deleting the DisaggregatedSet")
		_, err := kubectl.Delete("disaggregatedset", dsName).
			Namespace("default").
			RunQuiet()
		Expect(err).NotTo(HaveOccurred())

		By("verifying the scaler is garbage collected")
		Eventually(func(g Gomega) {
			out, _ := kubectl.Get("disaggregatedsetrolescaler", scalerName).
				Namespace("default").
				IgnoreNotFound().
				RunQuiet()
			g.Expect(strings.TrimSpace(out)).To(BeEmpty())
		}, 3*time.Minute, 2*time.Second).Should(Succeed())
	})
})
