class Logger:
    COLORS = {
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'magenta': '\033[95m',
        'cyan': '\033[96m',
        'white': '\033[97m',
        'reset': '\033[0m'
    }

    def log(self, message, color='reset'):
        color_code = self.COLORS.get(color, self.COLORS['reset'])
        print(f"{color_code}{message}{self.COLORS['reset']}")