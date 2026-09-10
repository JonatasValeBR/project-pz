import logging
import re
from datetime import date, datetime
from pathlib import Path

import pyotp
from selenium import webdriver
from selenium.common.exceptions import NoAlertPresentException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .config import Trf1Config


LOGGER = logging.getLogger(__name__)
PARTY_FILTERS = ("FAZENDA", "Delegado da Receita Federal")
MONTHS = {
    "jan": "01", "fev": "02", "mar": "03", "abr": "04",
    "mai": "05", "jun": "06", "jul": "07", "ago": "08",
    "set": "09", "out": "10", "nov": "11", "dez": "12",
}


def create_browser(config: Trf1Config) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if config.headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=pt-BR")
    return webdriver.Chrome(options=options)


class Trf1ProcessListScraper:
    def __init__(self, config: Trf1Config, browser=None) -> None:
        config.validate()
        self.config = config
        self.browser = browser or create_browser(config)
        self.wait = WebDriverWait(self.browser, config.timeout_seconds)

    def run(self, report_date: date) -> list[dict]:
        self._login()
        records: dict[str, dict] = {}
        for party_filter in PARTY_FILTERS:
            self.browser.get(self.config.reports_url)
            for record in self._search(party_filter, report_date):
                number = record["nr_processo"]
                if number in records:
                    existing_filters = set(records[number]["filtro"].split(" | "))
                    existing_filters.add(party_filter)
                    records[number]["filtro"] = " | ".join(sorted(existing_filters))
                else:
                    records[number] = record
        return list(records.values())

    def close(self) -> None:
        self.browser.quit()

    def save_screenshot(self, destination: Path) -> str:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.browser.save_screenshot(str(destination))
        return str(destination)

    def screenshot_bytes(self) -> bytes:
        return self.browser.get_screenshot_as_png()

    def _login(self) -> None:
        self.browser.get(self.config.endpoint)
        username = self.wait.until(
            EC.element_to_be_clickable((By.ID, "username"))
        )
        username.send_keys(self.config.login)
        password = self.wait.until(
            EC.element_to_be_clickable((By.ID, "password"))
        )
        password.send_keys(self.config.password)
        self.browser.find_element(By.ID, "kc-login").click()
        otp = self.wait.until(EC.element_to_be_clickable((By.ID, "otp")))
        otp.send_keys(pyotp.TOTP(self.config.totp_secret).now())
        self.browser.find_element(By.ID, "kc-login").click()
        self.wait.until(lambda driver: "ConsultaProcesso" in driver.current_url or
                        driver.find_elements(By.TAG_NAME, "body"))

    def _search(self, party_filter: str, report_date: date) -> list[dict]:
        input_name = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//input[@alt='Nome da Parte']"))
        )
        input_name.clear()
        input_name.send_keys(party_filter)
        self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="fPP:radioDecoration:radio:1"]')
            )
        ).click()
        date_text = report_date.strftime("%d/%m/%Y")
        for element_id in (
            "fPP:dataAutuacaoDecoration:dataAutuacaoInicioInputDate",
            "fPP:dataAutuacaoDecoration:dataAutuacaoFimInputDate",
        ):
            element = self.wait.until(EC.presence_of_element_located((By.ID, element_id)))
            self.browser.execute_script(
                "arguments[0].value = arguments[1];", element, date_text
            )
        search = self.wait.until(
            EC.element_to_be_clickable((By.ID, "fPP:searchProcessos"))
        )
        self.browser.execute_script("arguments[0].scrollIntoView(true);", search)
        search.click()
        self._wait_loading()

        records = []
        page = 1
        while True:
            LOGGER.info("Lendo TRF1: filtro=%s página=%s", party_filter, page)
            records.extend(self._read_page(party_filter))
            if not self._next_page():
                break
            page += 1
            self._wait_loading()
        return records

    def _read_page(self, party_filter: str) -> list[dict]:
        links = self.browser.find_elements(
            By.XPATH, '//a[@class="btn-link btn-condensed"]'
        )
        records = []
        for index in range(len(links)):
            current_links = self.browser.find_elements(
                By.XPATH, '//a[@class="btn-link btn-condensed"]'
            )
            link = current_links[index]
            process_number = re.sub(r"\D", "", link.get_attribute("textContent") or "")
            original_handles = set(self.browser.window_handles)
            link.click()
            try:
                self.browser.switch_to.alert.accept()
            except NoAlertPresentException:
                pass
            self.wait.until(
                lambda driver: len(set(driver.window_handles) - original_handles) == 1
            )
            detail_handle = (set(self.browser.window_handles) - original_handles).pop()
            self.browser.switch_to.window(detail_handle)
            try:
                records.append(self._read_details(process_number, party_filter))
            finally:
                self.browser.close()
                self.browser.switch_to.window(next(iter(original_handles)))
        return records

    def _read_details(self, process_number: str, party_filter: str) -> dict:
        class_text = self.wait.until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="maisDetalhes"]/dl/dd[1]'))
        ).get_attribute("textContent")
        details = self.browser.find_element(By.ID, "maisDetalhes")
        filed_text = details.find_element(
            By.XPATH, ".//dt[text()='Autuação']/following-sibling::dd[1]"
        ).get_attribute("textContent")
        section_text = self.browser.find_element(
            By.XPATH, '//*[@id="maisDetalhes"]/div[1]/dl/dd'
        ).get_attribute("textContent")
        match = re.search(r"(idProcesso|id)=(\d+)", self.browser.current_url)
        if not match:
            raise ValueError("ID do processo não encontrado na URL de detalhes")
        section_match = re.search(r"\b([A-Z]{2})\s*$", section_text.strip(), re.I)
        if not section_match:
            raise ValueError(f"Seção inválida nos detalhes do processo {process_number}")
        return {
            "id_processo": match.group(2),
            "nr_processo": process_number,
            "nr_classe_judicial": re.sub(r"\D", "", class_text or ""),
            "secao": section_match.group(1).lower(),
            "dt_autuacao": self._normalize_filed_date(filed_text),
            "link_acesso": self.config.endpoint,
            "filtro": party_filter,
        }

    @staticmethod
    def _normalize_filed_date(value: str) -> str:
        parts = value.strip().split()
        if len(parts) != 3 or parts[1].lower() not in MONTHS:
            raise ValueError(f"Data de autuação em formato inesperado: {value!r}")
        day, month, year = parts
        return f"{day.zfill(2)}/{MONTHS[month.lower()]}/{year}"

    def _next_page(self) -> bool:
        buttons = self.browser.find_elements(By.XPATH, "//td[contains(., '»')]")
        if not buttons:
            return False
        button = buttons[0]
        classes = button.get_attribute("class") or ""
        if "disabled" in classes:
            return False
        button.click()
        return True

    def _wait_loading(self) -> None:
        try:
            WebDriverWait(self.browser, self.config.timeout_seconds).until(
                lambda driver: (
                    driver.find_element(By.ID, "_viewRoot:status.start")
                    .value_of_css_property("display") == "none"
                )
            )
        except TimeoutException as exc:
            raise TimeoutError("TRF1 não concluiu o carregamento da consulta") from exc
