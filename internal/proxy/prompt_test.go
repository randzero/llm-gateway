package proxy

import "testing"

func TestExtractPromptText(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"openai-prompt", `{"prompt":"hello world"}`, "hello world"},
		{"openai-messages", `{"messages":[{"content":"a"},{"content":"b"}]}`, "ab"},
		{"openai-prompt-array", `{"prompt":["a","b"]}`, "ab"},
		{"anthropic", `{"model":"m","system":"sys","messages":[{"role":"user","content":"hi"}]}`, "syshi"},
		{
			"anthropic-blocks",
			`{"system":[{"type":"text","text":"s1"},{"type":"text","text":"s2"}],` +
				`"messages":[{"content":[{"type":"text","text":"c1"},{"type":"image","source":{}}]}]}`,
			"s1s2c1",
		},
		{"empty", `{}`, ""},
		{"garbage", `not json`, ""},
	}
	for _, c := range cases {
		if got := extractPromptText([]byte(c.body)); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}
