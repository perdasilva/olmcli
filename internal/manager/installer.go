package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/operator-framework/rukpak/api/v1alpha1"
	"github.com/perdasilva/olmcli/internal/resolution"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

type PackageInstaller struct {
	client   client.Client
	logger   *logrus.Logger
	resolver *resolution.OLMSolver
}

func NewPackageInstaller(resolver *resolution.OLMSolver, logger *logrus.Logger) (*PackageInstaller, error) {
	c, err := client.New(config.GetConfigOrDie(), client.Options{})
	if err != nil {
		return nil, err
	}
	if err := v1alpha1.AddToScheme(c.Scheme()); err != nil {
		return nil, err
	}
	return &PackageInstaller{
		client:   c,
		resolver: resolver,
		logger:   logger,
	}, nil
}

func (p *PackageInstaller) Install(ctx context.Context, requiredPackages ...*resolution.RequiredPackage) error {
	installables, err := p.Resolve(ctx, requiredPackages...)
	if err != nil {
		return err
	}
	for _, installable := range installables {
		if err := p.install(ctx, &installable); err != nil {
			return err
		}
	}
	return nil
}

func (p *PackageInstaller) Resolve(ctx context.Context, requiredPackages ...*resolution.RequiredPackage) ([]resolution.Installable, error) {
	p.logger.Debugf("resolving dependencies")
	start := time.Now()
	installables, err := p.resolver.Solve(ctx, requiredPackages...)
	if err != nil {
		p.logger.Fatal(err)
		return nil, err
	}
	elapsed := time.Since(start)
	p.logger.Debugf("took %s", elapsed)
	return installables, nil
}

func (p *PackageInstaller) install(ctx context.Context, installable *resolution.Installable) error {
	p.logger.Printf("Installing %s", installable.BundleID)
	bundleDeployment := p.bundleDeploymentFromInstallable(installable)
	if err := p.client.Create(ctx, bundleDeployment); err != nil {
		return err
	}
	return p.watchInstallation(ctx, client.ObjectKeyFromObject(bundleDeployment))
}

func (p *PackageInstaller) watchInstallation(ctx context.Context, bundleDeploymentKey client.ObjectKey) error {
	return retry.Do(func() error {
		bundleDeployment := &v1alpha1.BundleDeployment{}
		if err := p.client.Get(ctx, bundleDeploymentKey, bundleDeployment); err != nil {
			return err
		}
		p.logger.Printf("Deployment status conditions:")
		for _, condition := range bundleDeployment.Status.Conditions {
			p.logger.Printf("type: %s status: %s message: %s", condition.Type, condition.Status, condition.Message)
		}
		if meta.FindStatusCondition(bundleDeployment.Status.Conditions, v1alpha1.TypeInstalled) != nil {
			return nil
		}
		return fmt.Errorf("bundle not installed")
	}, retry.Context(ctx), retry.Attempts(10), retry.Delay(10*time.Second))
}

func (p *PackageInstaller) bundleDeploymentFromInstallable(installable *resolution.Installable) *v1alpha1.BundleDeployment {
	return &v1alpha1.BundleDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: installable.PackageName,
			Annotations: map[string]string{
				"annotations.olm.io/repository": installable.Repository,
				"annotations.olm.io/version":    installable.Version,
				"annotations.olm.io/channel":    installable.ChannelName,
			},
		},
		Spec: v1alpha1.BundleDeploymentSpec{
			ProvisionerClassName: "core-rukpak-io-plain",
			Template: &v1alpha1.BundleTemplate{
				Spec: v1alpha1.BundleSpec{
					ProvisionerClassName: "core-rukpak-io-registry",
					Source: v1alpha1.BundleSource{
						Type: v1alpha1.SourceTypeImage,
						Image: &v1alpha1.ImageSource{
							Ref:                 installable.GetBundlePath(),
							ImagePullSecretName: "regcred",
						},
					},
				},
			},
		},
	}
}
