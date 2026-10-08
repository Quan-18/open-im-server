package rpcsecure

import (
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStableNetworkDoesNotFallBackToPlaintext(t *testing.T) {
	_, err := ClientCredentials(config.RPCTLS{}, true)
	require.Error(t, err)
	_, err = ServerCredentials(config.RPCTLS{}, true)
	require.Error(t, err)
	_, err = ClientCredentials(config.RPCTLS{}, false)
	require.NoError(t, err)
}

func TestStableDiscoveryRejectsUnsupportedKubernetes(t *testing.T) {
	require.Error(t, ValidateDiscovery(true, config.KUBERNETES, ""))
	require.Error(t, ValidateDiscovery(true, "", config.KUBERNETES))
	require.NoError(t, ValidateDiscovery(false, config.KUBERNETES, ""))
	require.NoError(t, ValidateDiscovery(true, config.ETCD, ""))
}
