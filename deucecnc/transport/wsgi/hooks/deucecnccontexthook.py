import deucecnc


def DeucecncContextHook(req, resp, params):
    from threading import local as local_factory
    deucecnc.context = local_factory()
