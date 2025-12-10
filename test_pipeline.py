"""Test pipeline with error handling."""
import sys
import traceback
print("Starting pipeline test...")
print("Importing main...")
try:
    from run_all_competitors import main
    print("Main imported successfully")
except Exception as e:
    print(f"Import error: {e}")
    traceback.print_exc()
    sys.exit(1)

if __name__ == "__main__":
    print("Calling main()...")
    try:
        main()
        print("Main completed successfully")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)

