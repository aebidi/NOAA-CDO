# src/main.py
import argparse
from .pipelines import ghcnd_pipeline
from .pipelines import gsod_pipeline
from .pipelines import isd_pipeline
from .pipelines import normals_pipeline

def main():
    """Main function to control the data pipeline."""
    parser = argparse.ArgumentParser(description="CDO Station Data Download Pipeline")
    parser.add_argument(
        "--dataset",
        required=True,
        choices=["ghcnd", "gsod", "isd", "normals"],
        help="The dataset to process."
    )
    parser.add_argument(
        "--step",
        required=True,
        choices=["download", "process"],
        help="The pipeline step to run."
    )
    args = parser.parse_args()

    print(f"Running pipeline for dataset: {args.dataset}, step: {args.step}")

    if args.dataset == "ghcnd":
        ghcnd_pipeline.run_step(args.step)
    elif args.dataset == "gsod":
        gsod_pipeline.run_step(args.step)
    elif args.dataset == "isd":
        isd_pipeline.run_step(args.step)
    elif args.dataset == "normals":
        normals_pipeline.run_step(args.step)
    else:
        print(f"Pipeline for dataset '{args.dataset}' is not yet implemented.")

if __name__ == "__main__":
    main()