import psutil
import time
import logging

class ResourceMonitor:
    def __init__(self, check_interval=2.0, cpu_limit=90.0, ram_limit=90.0):
        self.check_interval = check_interval
        self.cpu_limit = cpu_limit
        self.ram_limit = ram_limit
        self.last_check = 0
        self.paused = False
        self.logger = logging.getLogger(__name__)

    def check(self):
        """Check system resources and pause if limits exceeded."""
        now = time.time()
        if now - self.last_check < self.check_interval:
            return

        self.last_check = now
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent

        if cpu > self.cpu_limit or ram > self.ram_limit:
            if not self.paused:
                self.logger.warning(f"⚠️ System Overload: CPU {cpu}% | RAM {ram}%. Throttling...")
                self.paused = True
            time.sleep(1) # active wait
        elif self.paused:
            self.logger.info("✅ System recovered. Resuming...")
            self.paused = False

    def get_stats(self):
        return f"CPU: {psutil.cpu_percent()}% | RAM: {psutil.virtual_memory().percent}%"
