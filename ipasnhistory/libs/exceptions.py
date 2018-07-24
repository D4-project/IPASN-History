#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class IPASNHistoryException(Exception):
    pass


class CreateDirectoryException(IPASNHistoryException):
    pass


class MissingEnv(IPASNHistoryException):
    pass


class InvalidDateFormat(IPASNHistoryException):
    pass
