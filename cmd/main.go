package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	taskmolev1alpha1 "github.com/andreasvikke/taskmole/api/v1alpha1"
	"github.com/andreasvikke/taskmole/internal/controller"
	"github.com/andreasvikke/taskmole/internal/mcpserver"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(taskmolev1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var namespace, mcpAddress, probeAddress string
	var leaderElection bool
	flag.StringVar(&namespace, "namespace", defaultNamespace(os.Getenv("WATCH_NAMESPACE")), "Namespace watched and exposed through MCP.")
	flag.StringVar(&mcpAddress, "mcp-bind-address", ":8082", "Streamable HTTP MCP address; use 0 to disable.")
	flag.StringVar(&probeAddress, "health-probe-bind-address", ":8081", "Health probe address.")
	flag.BoolVar(&leaderElection, "leader-elect", false, "Enable leader election.")
	logOptions := zap.Options{Development: true}
	logOptions.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&logOptions)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: probeAddress,
		LeaderElection: leaderElection, LeaderElectionID: "a3511f05.taskmole.io",
		Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{namespace: {}}},
	})
	if err != nil {
		setupLog.Error(err, "Failed to create manager")
		os.Exit(1)
	}
	if err := (&controller.WorkerProfileReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create WorkerProfile controller")
		os.Exit(1)
	}
	if err := (&controller.WorkerTaskReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create WorkerTask controller")
		os.Exit(1)
	}
	if mcpAddress != "0" {
		if err := mgr.Add(mcpserver.New(mcpAddress, namespace, mgr.GetClient())); err != nil {
			setupLog.Error(err, "Failed to add MCP server")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up readiness check")
		os.Exit(1)
	}
	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

func defaultNamespace(namespace string) string {
	if namespace == "" {
		return "taskmole"
	}
	return namespace
}
