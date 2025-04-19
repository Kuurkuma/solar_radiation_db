import logging

logger = logging.getLogger("main")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

def main():
    print("Hello from benchmarker!")


if __name__ == "__main__":
    main()
