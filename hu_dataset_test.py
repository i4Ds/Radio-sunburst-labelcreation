# %%
import pandas as pd
import glob
from sklearn.model_selection import train_test_split
from datasets import Dataset

# %%
BASE_PATH = "/mnt/nas05/data01/vincenzo/ecallisto/2014/"
images = glob.glob("{}*/*.parquet".format(BASE_PATH))

# %%
images[0]

# %%
df = pd.DataFrame()

# %%
df

# %%
df["image"] = images
df["antenna"] = df["image"].str.split("/", expand=True)[7].astype(str)
df["datetime"] = df["image"].str.split("/", expand=True)[8].str.replace(".parquet", "")
df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d_%H-%M-%S")

# %%
# Group by 'antenna' and get min, max datetime
min_max_datetime = df.groupby("antenna")["datetime"].agg(["min", "max"]).reset_index()

min_max_datetime

# %%
df.to_csv(
    f"{BASE_PATH}/train_metadata.csv",
    index=False,
)


# %%
# EDA
import matplotlib.pyplot as plt

# Number of images per antenna
images_per_antenna = df["antenna"].value_counts()

# Plotting
plt.figure(figsize=(10, 6))
images_per_antenna.plot(kind="bar")
plt.title("Number of Images per Antenna")
plt.xlabel("Antenna")
plt.ylabel("Number of Images")
plt.xticks(rotation=45)

plt.tight_layout()
plt.savefig(f"eda.png")
# Show the plot
plt.show()

# %%
from datasets import load_dataset

train = load_dataset(
    "csv",
    data_files=f"{BASE_PATH}/train_metadata.csv",
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
# %%
from datasets import Image

train = train.cast_column("image", Image())


# %%
train.push_to_hub("i4ds/ecallisto_radio_sunburst-2014", private=False)

# %%
# Display the image
plt.imshow(
    train["train"][7]["image"], cmap="gray"
)  # 'gray' colormap for mode=L (grayscale)
plt.title(
    f"Antenna: {train['train'][7]['antenna']} | Datetime: {train['train'][7]['datetime']}"
)
plt.axis("off")  # Turn off the axis
plt.show()
