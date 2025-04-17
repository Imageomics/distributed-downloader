# Initializers

Initializers are used to process the target dataset into a format that can be used by the downloader.
Spark is used to process the data in parallel.
The following steps are performed by the initializers:

1. Read the dataset from the source.
2. Filter the dataset to remove unwanted data.
3. Rename the columns to match the downloader's schema. Specifically:
    - `identifier` is the url to the image.
    - `source_id` some identification of the image, that can later be used to access relevant information from the
      source.
    - `license` (optional) the license of the image (needs to be an url to the license).
    - `source` (optional) source of the image, for licensing purposes.
    - `title` (optional) title of the image, for licensing purposes.
4. Extract server name from the `identifier` and generate `uuid` (unique internal identifier) for each image.
5. Partition the dataset first by the server name and then into smaller partitions with a fixed size (can be configured,
   default is 10,000)
6. Save the partitioned dataset to the target location in `parquet` format.

The initializers are run only once for each dataset, and the resulting partitioned dataset is used by the downloader to
download the images.

## Structure

### Base Initializer

The `BaseInitializer` class is the base class for all initializers. It contains all the common functionality used by all
initializers.
It has the following methods:

- `load_raw_df`: Reads the dataset from the source.
- `extract_server_name`: Extracts the server name from the `identifier`.
- `generate_uuid`: Generates a unique identifier for each image.
- `partition_dataframe`: Partitions the dataset first by the server name and then into smaller partitions with a fixed
  size.
- `save_results`: Saves the partitioned dataset to the target location in `parquet` format.

### Initializers

The initializers are classes that inherit from the `BaseInitializer` class. They implement the specific logic for each
dataset.
The following initializers are available:

- `GBIFInitializer`: Initializer for the GBIF dataset. It filters out any entries without an `gbifID` or `identifier`
  value.
  Additionally, removes any entries that are not `StillImage` by type or `image` by format.
  And lastly, it removes any entries that have `MATERIAL_CITATION` in `basisOfRecord`. This is because these are known to be images of text documents.
- `FathomNetInitializer`: Initializer for the FathomNet dataset. It filters out any entries without an `uuid` or `url`
  value.
  Additionally, removes any entries that are "not valid" by the `valid` column.
- `EoLInitializer`: Initializer for the EOL dataset. It filters out any entries without an `EOL content ID` or
  `EOL Full-Size Copy URL` value.
    - The `EOL content ID` is set as the `source_id`, which is used to map to the original metadata file to get the `EOL page ID` to match to the `taxon.tab` for taxa information. `EOL content ID` is not a persistent identifier at EOL, so it is important to maintain the original metadata file.
- `LilaInitializer`: Initializer for the LILA dataset. It filters out any entries that do not have a `url` value (by
  `url_gcp`, `url_aws` or `url_azure`).
  Additionally, removes any entries that are `empty` by the `original_label` column.

## Creating a new Initializer

To create a new initializer, you need to create a new class that inherits from the `BaseInitializer` class.
You will need to implement only `run` method, which ties together all the steps described above.
In the most cases you will need to implement only custom logic for filtering and renaming columns, everything else is
already implemented in the `BaseInitializer` class, and should be called in the order, described above.

### Important considerations before creating a new Initializer

- You need to be familiar with the dataset you are working with, and understand the structure of the data before
  starting to create a new initializer.
- `source_id` is highly recommended to be unique for each image, and be persistent across iterations of the dataset.
  Otherwise, it may be challenging to map additional information to the images. Or it may be challenging to update the
  dataset in the future.

### Required columns for proper `distributed-downloader` work

Initializer has to create a dataset with the following columns:
- `uuid` - unique internal identifier that is highly recommended to be generated on this step for later consistency
- `identifier` - url to the image
- `source_id` - some identification of the image that can later be used to access relative information from the
  source
