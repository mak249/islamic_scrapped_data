import os
import subprocess
import sys

# Define available pipelines and their scraper entry points
PIPELINES = {
    "1": ("Darussalam (Metadata)", "pipelines/darussalam/scraper.py"),
    "2": ("Salafi Publications (Metadata)", "pipelines/salafipublications/scraper.py"),
    "3": ("AbdurRahman.org (Content)", "pipelines/abdurrahman/scraper.py"),
    "4": ("Answering Hinduism (Content)", "pipelines/answeringhinduism/scraper.py"),
    "5": ("IslamQA Arabic (Content)", "pipelines/islamqa_ar/scraper.py"),
    "6": ("VedkaBhed (Content)", "pipelines/vedkabhed/scraper.py"),
    "7": ("YouTube RAG Pipeline", "pipelines/youtube_rag/runner.py"),
}

def main():
    while True:
        print("\n=============================================")
        print("      🚀 WEBSCRAPER PIPELINE LAUNCHER       ")
        print("=============================================")
        print("Select a pipeline to RESUME/START:\n")
        
        for key, (name, path) in PIPELINES.items():
            print(f"  [{key}] {name}")
            
        print("\n  [Q] Quit")
        
        choice = input("\nEnter selection: ").strip().upper()
        
        if choice == 'Q':
            print("Exiting...")
            break
            
        if choice in PIPELINES:
            name, script_path = PIPELINES[choice]
            
            # Verify file exists
            if not os.path.exists(script_path):
                print(f"\n❌ Error: Script not found at '{script_path}'")
                continue
                
            print(f"\n▶️ Launching {name}...")
            print(f"   Command: python {script_path}")
            print("---------------------------------------------\n")
            
            try:
                # Use sys.executable to ensure we use the same python interpreter
                subprocess.run([sys.executable, script_path], check=False)
            except KeyboardInterrupt:
                print("\n\n⏹️ Scraper stopped by user.")
            except Exception as e:
                print(f"\n❌ Error running script: {e}")
                
            input("\nPress Enter to return to menu...")
        else:
            print("\n❌ Invalid selection. Please try again.")

if __name__ == "__main__":
    # Ensure we are in the correct root directory (d:\web scraping)
    # This assumes the script is placed in the root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    main()
