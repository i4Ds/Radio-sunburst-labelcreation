# %%
import pandas as pd
import glob
from sklearn.model_selection import train_test_split
from datasets import Dataset

# %%
images = glob.glob(
    "/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/*/*/*.parquet"
)

# %%
images[0]

# %%
df = pd.DataFrame()

# %%
df

# %%
df["image"] = images
df["label"] = df["image"].str.split("/", expand=True)[8].astype(str)
df["antenna"] = df["image"].str.split("/", expand=True)[7].astype(str)
df["datetime"] = df["image"].str.split("/", expand=True)[9].str.replace(".parquet", "")
df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d_%H-%M-%S")


# %%
def custom_stratified_split(df, test_size=0.1, val_size=0.1, min_size=3):
    # Custom split function for a group
    def split_group(group_df):
        if len(group_df) < min_size:
            # For very small groups, include in training or distribute as best as possible
            if len(group_df) == 1:
                return group_df, pd.DataFrame(), pd.DataFrame()
            elif len(group_df) == 2:
                train = group_df.sample(n=1)
                return train, pd.DataFrame(), group_df.drop(train.index)
            else:
                train, test_val = train_test_split(
                    group_df, test_size=2, random_state=42
                )
                val, test = train_test_split(test_val, test_size=1, random_state=42)
                return train, val, test
        else:
            # Standard split for groups large enough
            train_val, test = train_test_split(
                group_df, test_size=test_size, random_state=42
            )
            train, val = train_test_split(
                train_val, test_size=val_size / (1 - test_size), random_state=42
            )
            return train, val, test

    grouped = df.groupby(["antenna", "label"])
    train_list: list[pd.DataFrame] = []
    val_list: list[pd.DataFrame] = []
    test_list: list[pd.DataFrame] = []

    for _, group in grouped:
        train, val, test = split_group(group)
        train_list.append(train)
        val_list.append(val)
        test_list.append(test)

    # Concatenate all the splits back into DataFrames
    train_df = pd.concat(train_list)
    val_df = pd.concat(val_list)
    test_df = pd.concat(test_list)

    return train_df, val_df, test_df


# Splitting the DataFrame
train_df, val_df, test_df = custom_stratified_split(df)


# %%
# Group by 'antenna' and get min, max datetime
min_max_datetime = df.groupby("antenna")["datetime"].agg(["min", "max"]).reset_index()

min_max_datetime


# %%
# Custom function to assert no overlapping file_names between splits
def assert_no_overlap(train_df, val_df, test_df):
    train_files = set(train_df["image"])
    val_files = set(val_df["image"])
    test_files = set(test_df["image"])

    assert train_files.isdisjoint(val_files), "Train and Validation sets overlap."
    assert train_files.isdisjoint(test_files), "Train and Test sets overlap."
    assert val_files.isdisjoint(test_files), "Validation and Test sets overlap."


# Perform the assertion check
assert_no_overlap(train_df, val_df, test_df)

# %%
val_df

# %%
train_df.to_csv(
    "/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/train_metadata.csv",
    index=False,
)
test_df.to_csv(
    "/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/test_metadata.csv",
    index=False,
)
val_df.to_csv(
    "/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/val_metadata.csv",
    index=False,
)

# %%
# EDA
import matplotlib.pyplot as plt

# Number of images per antenna
images_per_antenna = test_df["antenna"].value_counts()

# Number of burst types per antenna
burst_types_per_antenna = (
    test_df.groupby("antenna")["label"].value_counts().unstack(fill_value=0)
)

# Date range per antenna
date_range_per_antenna = test_df.groupby("antenna")["datetime"].agg([min, max])

# Plotting
fig, ax = plt.subplots(2, 1, figsize=(6, 12))

# Images per antenna
images_per_antenna.plot(kind="bar", ax=ax[0], color="skyblue")
ax[0].set_title("Number of Images per Antenna")
ax[0].set_xlabel("Antenna")
ax[0].set_ylabel("Number of Images")

# Burst types per antenna
burst_types_per_antenna.plot(kind="bar", stacked=True, ax=ax[1])
ax[1].set_title("Burst Types per Antenna")
ax[1].set_xlabel("Antenna")
ax[1].set_ylabel("Count")

plt.tight_layout()
plt.show()

# %%
from datasets import load_dataset

train = load_dataset(
    "csv",
    data_files="/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/train_metadata.csv",
)
valid = load_dataset(
    "csv",
    data_files="/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/val_metadata.csv",
)
test = load_dataset(
    "csv",
    data_files="/mnt/nas05/data01/vincenzo/ecallisto/hu_dataset_live_mai_october/test_metadata.csv",
)

# %%
import pandas as pd
from PIL import Image as PILImage
import io


def load_image_from_parquet(example):
    parquet_path = example["image"]  # Path to the Parquet file
    # Read the Parquet file
    df = pd.read_parquet(parquet_path)
    # Convert bytes to PIL Image
    image = PILImage.fromarray(df.values.T)
    # Update the example with the image
    example["image"] = image
    return example


# %%
# Apply the function to convert the 'image' column
train = train.map(load_image_from_parquet)
valid = valid.map(load_image_from_parquet)
test = test.map(load_image_from_parquet)

# %%
from datasets import Image

train = train.cast_column("image", Image())
valid = valid.cast_column("image", Image())
test = test.cast_column("image", Image())

# %%
from datasets import DatasetDict

dd = DatasetDict(
    {"train": train["train"], "validation": valid["train"], "test": test["train"]}
)


# %%
dd.push_to_hub("i4ds/ecallisto_radio_sunburst-mai-october", private=False)

# %%
# Display the image
plt.imshow(
    dd["train"][7]["image"], cmap="gray"
)  # 'gray' colormap for mode=L (grayscale)
plt.title(
    f"Antenna: {dd['train'][7]['antenna']} | Datetime: {dd['train'][7]['datetime']} | Label: {dd['train'][7]['label']}"
)
plt.axis("off")  # Turn off the axis
plt.show()
