package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"testing"
	"time"
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		expected string
		input    string
	}{
		{"2025-01-09T20:27:27.236Z", "[2025-01-09 20:27:27,236] [sidecar-bg-task 3850964] [140300208650704] [metrics_ns:408] [INFO   ] Producer update metric time: 2025-01-09 20:27:27.236157"},
		{"2025-01-15T17:21:51.292Z", "2025-01-15 17:21:51,292:3239354(0x7fa656b3d640):ZOO_INFO@log_env@1250: Client environment:zookeeper.version=zookeeper C client 3.7.0"},
		{"2025-01-15T19:29:15.46331Z", "I20250115 19:29:15.463310 3239941 glogger.cpp:61] conf_negotiate_server.cpp:196 [CACHE_CORE][INFO] The request node(172.16.0.33) not in cluster view"},
		{"2025-01-15T19:29:15.686245Z", "E20250115 19:29:15.686245 3239482 glogger.cpp:71] delegation_token_mgr.cpp:116 [CACHE_CORE][ERROR] Verify token failed, token has expired, expired time is 1736940381, current time is 1736940555."},
		{"2025-01-15T05:26:33.179Z", "2025-01-15 05:26:33,179 | INFO | sidecar-instance-check.sh:53 | Running sidecar-instance-check.sh."},
		{"2025-01-15T19:11:07Z", "25-1-15 19:11:07[INFO][3239354 KeCallbackDestroyThreadLock:303]CallBackDestroyThreadLock completed"},
		{"2025-01-15T05:24:59.93Z", "2025-01-15 05:24:59,930 | INFO | sidecar-instance-check.sh:63 | SideCar Health Status normal."},
		{"2024-12-23T15:47:50Z", "2024-12-23 15:47:50 [INFO] ./install/install_vm_mrs.sh: 307  delete cache directory: /srv/BigData/data1/memarts data successfullly!"},
		{"2024-12-23T15:55:08Z", "========== 2024-12-23 15:55:08 start nodemanager by NORMAL =========="},
		{"2025-01-07T22:46:00Z", "2025-01-07 22:46:00"},
		{"2024-12-23T15:55:26.569+08:00", "2024-12-23T15:55:26.569+0800: 1.138: [GC (Allocation Failure) 2024-12-23T15:55:26.569+0800: 1.138: [ParNew: 104960K->8530K(118016K), 0.0108196 secs] 104960K->8530K(511232K), 0.0109303 secs] [Times: user=0.02 sys=0.01, real=0.01 secs]"},
		{"2025-01-02T01:16:55Z", "2025-01-02 01:16:55 GC log file created /var/log/Bigdata/yarn/nm/nodemanager-omm-20241223155524-pid154200-gc.log.4"},
		{"2025-01-15T19:23:42.042Z", "2025-01-15 19:23:42,042 | WARN  | ContainerLocalizer #0 | Exception encountered while connecting to the server  | Client.java:756"},
		{"2025-01-15T19:23:49.752+08:00", "2025-01-15T19:23:49.752+0800: 1.412: [GC (Allocation Failure) [PSYoungGen: 128512K->12717K(149504K)] 128512K->12725K(491008K), 0.0111485 secs] [Times: user=0.03 sys=0.01, real=0.01 secs] "},
		{"2025-01-15T19:24:08-08:00", "2025-01-15 19:24:08-08:00 | INFO  | [139837877704256] shard_view_mgt.cpp:109 [SHARD_VIEW][INFO] Update view success, version is 117."},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ts := ParseTimestamp([]byte(tt.input))
			actual := ts.Format(time.RFC3339Nano)
			assertEquals(t, tt.expected, actual)
		})
	}
}
