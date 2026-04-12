import logging
from pyrogram.session import Session
import inspect

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("diagnostic")

def get_session_signature():
    try:
        sig = inspect.signature(Session.__init__)
        log.info(f"Session.__init__ signature: {sig}")
        
        # Check if it's inherited
        log.info(f"Session MRO: {Session.mro()}")
        
    except Exception as e:
        log.error(f"Failed to get signature: {e}")

if __name__ == "__main__":
    get_session_signature()
