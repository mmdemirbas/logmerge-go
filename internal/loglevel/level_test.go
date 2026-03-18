package loglevel_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/loglevel"
	"github.com/mmdemirbas/logmerge/internal/testutil"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		tsStart int
		tsEnd   int
		want    Level
	}{
		// ── Pipe-delimited (Spark/sidecar logs) ──────────────────────
		{"pipe INFO", "2025-01-15 10:00:00,179 | INFO | msg", 0, 24, Info},
		{"pipe WARN", "2025-01-15 10:00:00,179 | WARN | msg", 0, 24, Warn},
		{"pipe ERROR", "2025-01-15 10:00:00,179 | ERROR | msg", 0, 24, Error},
		{"pipe DEBUG", "2025-01-15 10:00:00,179 | DEBUG | msg", 0, 24, Debug},
		{"pipe padded INFO", "2025-01-15 10:00:00,179 | INFO  | msg", 0, 24, Info},

		// ── Bracketed levels ─────────────────────────────────────────
		{"bracket ERROR", "[2025-01-09 20:27:27,236] [ERROR] [main] msg", 0, 27, Error},
		{"bracket DEBUG", "[2025-01-09 20:27:27,236] [DEBUG]: PID:(123)", 0, 27, Debug},
		{"bracket INFO", "[2025-01-09 20:27:27,236] [INFO] msg", 0, 27, Info},
		{"bracket NOTICE", "[2025-01-09 20:27:27,236][][][NOTICE][msg]", 0, 26, Notice},

		// ── glog single-letter prefix ─────────────────────────────────
		{"glog I", "I20250115 19:29:15.463310 3239941 glogger.cpp", 1, 25, Info},
		{"glog E", "E20250115 19:29:15.686245 3239482 glogger.cpp", 1, 25, Error},
		{"glog W", "W20250115 19:29:15.000000 1234 file.cpp", 1, 25, Warn},
		{"glog F", "F20250115 19:29:15.000000 1234 file.cpp", 1, 25, Fatal},

		// ── Hadoop/YARN bare word after timestamp ─────────────────────
		{"hadoop INFO", "2026-03-15 16:13:30,029 INFO conf.Configuration: found", 0, 24, Info},
		{"hadoop WARN", "2026-03-15 16:13:30,029 WARN conf.Configuration: deprecated", 0, 24, Warn},

		// ── Bare word after space ─────────────────────────────────────
		{"bare INFO", "2024-12-23 15:47:50 [INFO] ./install.sh: 307", 0, 20, Info},
		{"bare ERROR", "2024-12-23 15:47:50 [ERROR] something failed", 0, 20, Error},

		// ── Case insensitive ─────────────────────────────────────────
		{"lowercase info", "2025-01-15 10:00:00 info message", 0, 20, Info},
		{"lowercase warn", "2025-01-15 10:00:00 warn message", 0, 20, Warn},
		{"mixed case Info", "2025-01-15 10:00:00 Info message", 0, 20, Info},

		// ── Normalized aliases ─────────────────────────────────────────
		{"WARNING → WARN", "2025-01-15 10:00:00 WARNING message", 0, 20, Warn},
		{"SEVERE → ERROR", "2025-01-15 10:00:00 SEVERE message", 0, 20, Error},
		{"CRITICAL → FATAL", "2025-01-15 10:00:00 CRITICAL message", 0, 20, Fatal},
		{"NOTICE", "2025-01-15 10:00:00 NOTICE message", 0, 20, Notice},
		{"TRACE", "2025-01-15 10:00:00 TRACE message", 0, 20, Trace},
		{"FATAL", "2025-01-15 10:00:00 FATAL message", 0, 20, Fatal},

		// ── No level detected ─────────────────────────────────────────
		{"no level - continuation", "  at com.example.Main(Main.java:42)", 0, 0, Unknown},
		{"no level - plain text", "some random log line without level", 0, 0, Unknown},
		{"no level - only timestamp", "2025-01-15 10:00:00 3239941 glogger.cpp", 0, 20, Unknown},

		// ── Don't match partial words ─────────────────────────────────
		{"no match INFORMATION", "2025-01-15 10:00:00 INFORMATION msg", 0, 20, Unknown},
		{"no match ERRORS", "2025-01-15 10:00:00 ERRORS msg", 0, 20, Unknown},
		{"no match WARNING2", "2025-01-15 10:00:00 WARNING2 msg", 0, 20, Unknown},
		{"no match INFORMED", "2025-01-15 10:00:00 INFORMED msg", 0, 20, Unknown},
		{"no match DEBUGGER", "2025-01-15 10:00:00 DEBUGGER msg", 0, 20, Unknown},
		{"no match TRACED", "2025-01-15 10:00:00 TRACED msg", 0, 20, Unknown},
		{"no match FATALITY", "2025-01-15 10:00:00 FATALITY msg", 0, 20, Unknown},
		{"no match NOTICE_ID", "2025-01-15 10:00:00 NOTICE_ID msg", 0, 20, Unknown},
		{"no match SEVERITY", "2025-01-15 10:00:00 SEVERITY msg", 0, 20, Unknown},
		{"no match WARN3", "2025-01-15 10:00:00 WARN3 msg", 0, 20, Unknown},
		{"no match ERROR_CODE", "2025-01-15 10:00:00 ERROR_CODE msg", 0, 20, Unknown},

		// ── glog false positives: letter prefix must be at line start ─
		{"no glog mid-word I", "xI20250115 19:29:15.463310 3239941", 2, 26, Unknown},
		{"no glog after letter", "XI20250115 19:29:15.463310 3239941", 2, 26, Unknown},
		{"glog after space is ok", " I20250115 19:29:15.463310 3239941", 2, 26, Info},

		// ── Level word appears in message, not in level position ──────
		{"ERROR in message only", "2025-01-15 10:00:00 3239941 got ERROR in request", 0, 20, Unknown},
		{"WARN far from timestamp", "2025-01-15 10:00:00 some very long prefix text before WARN appears", 0, 20, Unknown},

		// ── Multiple level-like words: first one wins ────────────────
		{"first level wins", "2025-01-15 10:00:00 WARN ERROR msg", 0, 20, Warn},
		{"first level wins bracket", "[2025-01-09 20:27:27] [INFO] [ERROR] msg", 0, 22, Info},

		// ── Edge cases with empty/short lines ────────────────────────
		{"empty line", "", 0, 0, Unknown},
		{"just a newline", "\n", 0, 0, Unknown},
		{"tsEnd at end of line", "2025-01-15 10:00:00", 0, 19, Unknown},
		{"tsEnd past line", "2025-01-15 10:00:00", 0, 50, Unknown},
		{"single char line", "X", 0, 0, Unknown},

		// ── Level immediately after timestamp (no space) ─────────────
		{"level glued to timestamp", "2025-01-15 10:00:00INFO msg", 0, 19, Info},
		{"level glued with bracket", "2025-01-15 10:00:00[INFO] msg", 0, 19, Info},

		// ── Levels with various delimiters ────────────────────────────
		{"level after colon", "2025-01-15 10:00:00: INFO msg", 0, 20, Info},
		{"level after tab", "2025-01-15 10:00:00\tINFO\tmsg", 0, 19, Info},
		{"level after multiple pipes", "2025-01-15 10:00:00 || INFO || msg", 0, 20, Info},
		{"level in parens", "2025-01-15 10:00:00 (ERROR) msg", 0, 20, Error},
		{"level after mixed delims", "2025-01-15 10:00:00 | [WARN] | msg", 0, 20, Warn},

		// ── Level with newline boundary ──────────────────────────────
		{"level before newline", "2025-01-15 10:00:00 INFO\n", 0, 20, Info},
		{"no scan past newline", "2025-01-15 10:00:00 data\nINFO msg", 0, 20, Unknown},

		// ── Syslog-style prefix before timestamp ─────────────────────
		{"syslog prefix no glog", "<165> 2024-08-04T12:00:01Z INFO msg", 6, 27, Info},

		// ── Java exception lines (no level) ──────────────────────────
		{"java stack trace", "\tat java.lang.Thread.run(Thread.java:748)", 0, 0, Unknown},
		{"caused by", "Caused by: java.io.IOException: Connection reset", 0, 0, Unknown},
		{"continuation indent", "    at org.apache.spark.executor.Executor(Executor.scala:42)", 0, 0, Unknown},

		// ── GC log tags (not levels) ─────────────────────────────────
		{"gc tag not a level", "[2026-03-09T20:37:17.105+0800][gc,init] CardTable", 0, 34, Unknown},

		// ── Python logging format ────────────────────────────────────
		{"python WARNING", "2025-01-15 10:00:00 WARNING msg", 0, 20, Warn},
		{"python CRITICAL", "2025-01-15 10:00:00 CRITICAL msg", 0, 20, Fatal},

		// ── Shell script formats ─────────────────────────────────────
		// Level after non-level content (script path) is not detected to avoid
		// false positives. The first non-delimiter, non-level word stops the scan.
		{"shell INFO after context - not detected", "CST 2026-03-13 17:38:40 [omsconfig.sh:867] INFO Start config", 4, 24, Unknown},
		// But if the level is right after the timestamp, it works
		{"shell INFO right after ts", "CST 2026-03-13 17:38:40 INFO Start config", 4, 24, Info},

		// ── Real log lines from examples ─────────────────────────────
		{
			"real: spark pipe",
			"2025-01-15 05:26:33,179 | INFO | sidecar-instance-check.sh:53 | Running check",
			0, 24, Info,
		},
		{
			"real: glog info",
			"I20260313 18:51:27.790683 993466 glogger.cpp:55] Init glog success.",
			1, 26, Info,
		},
		{
			"real: glog error",
			"E20260312 16:16:53.329900 3944469 glogger.cpp:71] secret_mgr.cpp:515 [CACHE_CORE][ERROR]",
			1, 26, Error,
		},
		{
			"real: bracketed error",
			"[2026-03-13 17:45:15,995] [ERROR] [main] [FmsModule:initActiveAlarmAll 595] return",
			0, 27, Error,
		},
		{
			"real: script debug",
			"[2026-03-13 17:45:57] [DEBUG]: PID:(1671493): Enter main.",
			0, 22, Debug,
		},
		{
			"real: ha notice",
			"[2026-03-13 17:45:45][][][NOTICE][HA Monitor is Starting]",
			0, 22, Notice,
		},
		{
			"real: hadoop info",
			"2026-03-15 16:13:30,029 INFO conf.Configuration: found resource",
			0, 24, Info,
		},
		{
			"real: user log line",
			"2026-03-15 23:29:42-08:00 | INFO  | [140228373800512] access_sdk.cpp:549",
			0, 26, Info,
		},
		{
			"real: small example WARN",
			"2025-01-01 02:30:00,500 | WARN  | Connection to database timed out after 30s",
			0, 24, Warn,
		},
		{
			"real: small example ERROR",
			"2025-01-01 08:00:00,000 | ERROR | Unhandled exception in request handler",
			0, 24, Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseLevel([]byte(tt.line), tt.tsStart, tt.tsEnd)
			testutil.AssertEquals(t, tt.want, result.Level)
		})
	}
}

func TestParseLevel_Positions(t *testing.T) {
	tests := []struct {
		name      string
		line      string
		tsStart   int
		tsEnd     int
		wantLevel Level
		wantStart int
		wantEnd   int
	}{
		{
			"pipe level position",
			//                         24:'|' 25:' ' 26:'I' 27:'N' 28:'F' 29:'O' 30:' '
			"2025-01-15 10:00:00,179 | INFO | msg",
			0, 24,
			Info, 26, 31,
		},
		{
			"bracket level position",
			//                          27:'E' 28:'R' 29:'R' 30:'O' 31:'R' 32:']'
			"[2025-01-09 20:27:27,236] [ERROR] msg",
			0, 27,
			Error, 27, 33,
		},
		{
			"glog prefix position",
			"I20250115 19:29:15.463310 3239941",
			1, 25,
			Info, 0, 1,
		},
		{
			"bare word after space",
			"2025-01-15 10:00:00 INFO msg",
			0, 20,
			Info, 20, 25,
			// " INFO " → "INFO" at 20-24, trailing ' ' advances to 25
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseLevel([]byte(tt.line), tt.tsStart, tt.tsEnd)
			testutil.AssertEquals(t, tt.wantLevel, result.Level)
			testutil.AssertEquals(t, tt.wantStart, result.Start)
			testutil.AssertEquals(t, tt.wantEnd, result.End)
		})
	}
}

func TestLevel_Label(t *testing.T) {
	tests := []struct {
		level Level
		want  string
	}{
		{Unknown, "      "},
		{Trace, "TRACE "},
		{Debug, "DEBUG "},
		{Info, "INFO  "},
		{Notice, "NOTE  "},
		{Warn, "WARN  "},
		{Error, "ERROR "},
		{Fatal, "FATAL "},
	}
	for _, tt := range tests {
		t.Run(tt.level.String(), func(t *testing.T) {
			testutil.AssertEquals(t, tt.want, string(tt.level.Label()))
			testutil.AssertEquals(t, 6, len(tt.level.Label()))
		})
	}
}
