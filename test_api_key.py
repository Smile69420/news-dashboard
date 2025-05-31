import os
import google.generativeai as genai
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_gemini_api_key():
    """
    Tests the Gemini API key by trying to list available models.
    """
    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        print("Error: GEMINI_API_KEY not found in .env file or environment variables.")
        return

    print(f"Found API Key: {api_key[:10]}... (partially hidden for security)") # Print a portion

    try:
        genai.configure(api_key=api_key)
        print("\nAttempting to list models...")
        models_found = False
        for m in genai.list_models():
            # Check if the model supports 'generateContent' which your script uses
            if 'generateContent' in m.supported_generation_methods:
                print(f"  - Model: {m.name} (supports generateContent)")
                models_found = True

        if models_found:
            print("\nSUCCESS: API key seems valid and models supporting 'generateContent' are available.")
            print("You should be able to use models like 'gemini-1.0-pro'.")
        else:
            print("\nWARNING: API key might be valid, but no models supporting 'generateContent' were found.")
            print("Please check your project setup and available models in the Google Cloud Console.")

    except Exception as e:
        print(f"\nERROR: An issue occurred while trying to use the API key.")
        print(f"  Details: {e}")
        print("  Possible reasons:")
        print("    - The API key is invalid or has been revoked.")
        print("    - The Generative Language API is not enabled for your project in Google Cloud Console.")
        print("    - Billing is not set up for your project (if required for the API).")
        print("    - Incorrect project association with the API key.")
        print("    - Network connectivity issues.")

if __name__ == "__main__":
    test_gemini_api_key()
