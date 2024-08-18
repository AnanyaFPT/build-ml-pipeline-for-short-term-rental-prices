#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd



logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # load data to be cleaned
    logger.info('loading input data for cleaning from '+ artifact_local_path)
    df = pd.read_csv(artifact_local_path)

    # drop outliers
    min_price = args.min_price
    max_price = args.max_price
    logger.info('dropping outliers; only keeping data with prices between '+ str(min_price) +' and '+ str(max_price))
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info('converting last_review column to date type')
    df['last_review'] = pd.to_datetime(df['last_review'])

    # save cleaned data to file 
    logger.info('saving cleaned data to clean_sample.csv')
    df.to_csv("clean_sample.csv", index=False)

    # create artifact add cleaned file and log it in wandb
    logger.info('creating and logging cleaned data as artifact in wandb')
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    # end run 
    run.finish()

    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="the data artifact we want to clean",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="the data artifact generated as the result of cleaning ",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="the type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="a description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="the minimum price to consider valid in dataset",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="the maximum price to consider valid in dataset",
        required=True
    )


    args = parser.parse_args()

    go(args)
