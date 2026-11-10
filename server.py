import contextlib
import signal
import threading
import uvicorn


class LabServer(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self):
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        handled=(signal.SIGINT,signal.SIGTERM)
        original={sig:signal.signal(sig,self.handle_exit) for sig in handled}
        try:
            yield
        finally:
            for sig,handler in original.items():signal.signal(sig,handler)

        


if __name__=='__main__':
    config=uvicorn.Config('app:create_app',factory=True,host='0.0.0.0',port=8000,
                          workers=1,timeout_graceful_shutdown=8,access_log=False)
    LabServer(config).run()
