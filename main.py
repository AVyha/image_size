import PIL
import httpx

import asyncio

from PIL import Image
from io import BytesIO

from httpx import AsyncClient

from get_sheets import get_sheet, get_data_from_sheet


def write_data_to_sheet(df, sheet, client):
    df = df.replace("", "Invalid url")
    df.to_csv("image_and_size.csv", index=False)

    with open("image_and_size.csv", "r") as file_obj:
        content = file_obj.read()
        client.import_csv(sheet.id, data=content)


def write_size_to_df(index, size):
    global data
    data["SIZE"][index] = size


async def write_img_size(session, index, url):
    try:
        response = await session.get(url, timeout=600.0)

        img = Image.open(BytesIO(response.content))

        width, height = img.size
        img_size = f"{width}x{height}"
        write_size_to_df(index, img_size)
    except (
            PIL.UnidentifiedImageError,
            TypeError,
            ValueError,
            httpx.ReadError,
            httpx.UnsupportedProtocol,
            httpx.RemoteProtocolError,
            httpx.ConnectError
    ):
        pass


async def load_img_size(data):
    async with AsyncClient() as session:
        tasks = []
        for i in data.index:
            task = asyncio.create_task(write_img_size(session, i, data["image_url"][i]))
            tasks.append(task)

        await asyncio.gather(*tasks)


if __name__ == '__main__':
    client, sheet = get_sheet()
    data = get_data_from_sheet(sheet)

    asyncio.run(load_img_size(data))

    write_data_to_sheet(data, sheet, client)
