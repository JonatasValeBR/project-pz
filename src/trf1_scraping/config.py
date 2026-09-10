from dataclasses import dataclass
from pathlib import Path
from typing import Union


@dataclass(frozen=True)
class Trf1Config:
    endpoint: str
    login: str
    password: str
    totp_secret: str
    report_dir: Union[Path, str]
    screenshot_dir: Path
    reports_url: str = (
        "https://pje1g.trf1.jus.br/pje/Processo/ConsultaProcesso/listView.seam"
    )
    timeout_seconds: int = 60
    headless: bool = True

    def validate(self) -> None:
        required = {
            "endpoint": self.endpoint,
            "login": self.login,
            "password": self.password,
            "totp_secret": self.totp_secret,
            "reports_url": self.reports_url,
        }
        missing = [name for name, value in required.items() if not str(value).strip()]
        if missing:
            raise ValueError(
                "Configuração TRF1 incompleta: " + ", ".join(sorted(missing))
            )
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds deve ser maior que zero")
