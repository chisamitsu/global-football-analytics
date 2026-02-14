from src.load.load_dimensions import load_dimensions
from src.load.load_facts import load_facts

def run_load_pipeline():
    print("Starting Load Layer...")
    load_dimensions()
    load_facts()
    print("Load Layer completed successfully.")

if __name__ == "__main__":
    run_load_pipeline()