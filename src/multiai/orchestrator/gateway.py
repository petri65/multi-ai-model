from dataclasses import dataclass

@dataclass
class ChangeProposal:
    shards: list[str]
    title: str
    description: str

class Orchestrator:
    def prepare(self, cp: ChangeProposal) -> None:
        print("prepare:", cp.title)
    def validate_local(self) -> None:
        print("validate local: Llama-Guard, Protocol-Auditor, GPT-Math-Validate")
    def open_pr(self) -> None:
        print("open PR (GitHub App to be wired)")
