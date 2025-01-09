import os
import json
import argparse
from openai import OpenAI

def finetune(training_file, suffix, base_model='gpt-3.5-turbo', n_epochs=4):
    api_key = [c['api_key'] for c in json.load(open('OAI_CONFIG_LIST')) if c['model'] == base_model][0]
    client = OpenAI(api_key=api_key)

    # Upload the training file to the OpenAI API
    file = client.files.create(
        file=open(training_file, "rb"),
        purpose="fine-tune"
    )

    # Create a fine-tuning job
    client.fine_tuning.jobs.create(
        training_file=file.id,
        model=base_model,
        suffix=suffix,
        hyperparameters={
            "n_epochs": n_epochs
        }
    )


def main():
    # Read arguments from the command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--training_file", type=str, help="Path to the training file.")
    parser.add_argument("--base_model", type=str, default='gpt-3.5-turbo', help="Base model for fine-tuning.")
    parser.add_argument("--n_epochs", type=int, default=4, help="Number of epochs for fine-tuning.")
    parser.add_argument("--suffix", type=str, default='CMS_LENS', help="Suffix for the fine-tuned model.")
    args = parser.parse_args()
    filename = args.training_file
    print(f"Fine-tuning the model with the file: {filename}")
    finetune(filename, args.suffix, base_model=args.base_model, n_epochs=args.n_epochs)

if __name__ == "__main__":
    main()

