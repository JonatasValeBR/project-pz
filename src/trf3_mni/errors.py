class ApplicationError(Exception):
    """Erro conhecido e seguro para exibição no resumo da execução."""


class ConfigurationError(ApplicationError):
    pass


class IntegrationError(ApplicationError):
    pass


class InvalidResponseError(IntegrationError):
    pass


class ProcessNotFoundError(ApplicationError):
    pass


class DocumentSelectionError(ApplicationError):
    pass


class StorageError(ApplicationError):
    pass
