#!/usr/bin/env python3


class IPASNHistoryException(Exception):
    pass


class MissingEnv(IPASNHistoryException):
    pass


class CreateDirectoryException(IPASNHistoryException):
    pass


class ConfigError(IPASNHistoryException):
    pass
