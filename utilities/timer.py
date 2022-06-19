import time
class Timer():
    def __init__(self, identifier):
        self.end = None
        self.start = None
        self.elapsed_time = None
        self.identifier = identifier
    
    def results(self):
        completed_message = f'Completed in {self.elapsed_time} seconds'
        if self.identifier:
            return f'{self.identifier}: {completed_message}'
        return completed_message

    def reset(self):
        self.start = None
        self.end = None
        self.elapsed_time = None

    def begin(self):
        self.start = time.time()

    def stop(self):
        self.end = time.time()
        self.elapsed_time = round(self.end - self.start, 3)
