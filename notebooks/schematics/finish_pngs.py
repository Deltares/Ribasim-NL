# %%
from pathlib import Path

import numpy as np
from PIL import Image, ImageChops

legend_positions = {"noorderzijlvest": "rb", "aaenmaas": "lb"}


def strip_frame_by_contrast(img, strip_border_px=5):
    """Fix border with `strip_border_px` amount of pixels."""
    arr = np.array(img.convert("RGB"))
    top, bottom = 0, arr.shape[0]
    left, right = 0, arr.shape[1]

    return img.crop(
        (left + strip_border_px, top + strip_border_px, right - (2 * strip_border_px), bottom - (2 * strip_border_px))
    )


def crop_white_margins(img, threshold=4, padding=12, bg_color=(255, 255, 255)):
    """Crop white margins from picture."""
    im = img.convert("RGBA")
    bg = Image.new("RGBA", im.size, (*bg_color, 255))

    diff = ImageChops.difference(im, bg).convert("L")

    # Alles met diff > threshold telt als inhoud
    mask = diff.point(lambda p: 255 if p > threshold else 0)
    bbox = mask.getbbox()
    if not bbox:
        return im

    left, top, right, bottom = bbox
    left = max(0, left - padding)
    top = max(0, top - padding)
    right = min(im.width, right + padding)
    bottom = min(im.height, bottom + padding)
    return im.crop((left, top, right, bottom))


def paste_legend(img, legend_path, pos="lb", margin=(12, 12)):
    """Paste legend in picture."""
    base = img.convert("RGBA")
    legend = Image.open(legend_path).convert("RGBA")
    mx, my = margin

    # Als legenda te groot is, schaal 'm passend (veilig)
    if legend.width + 2 * mx > base.width or legend.height + 2 * my > base.height:
        scale = min((base.width - 2 * mx) / legend.width, (base.height - 2 * my) / legend.height, 1.0)
        new_w = max(1, round(legend.width * scale))
        new_h = max(1, round(legend.height * scale))
        legend = legend.resize((new_w, new_h), Image.Resampling.LANCZOS)

    if pos == "lb":
        x = mx
        y = base.height - legend.height - my
    elif pos == "rb":
        x = base.width - legend.width - mx
        y = base.height - legend.height - my
    elif pos == "rt":
        x = base.width - legend.width - mx
        y = my
    elif pos == "lt":
        x = mx
        y = my
    else:
        raise ValueError("pos must be one of: lb, rb, rt, lt")

    base.paste(legend, (x, y), legend)
    return base


def crop_and_add_legend(
    image_path,
    out_path,
    legend_path=None,
    legend_pos="lb",
    white_threshold=15,
    white_padding=4,
    legend_margin=(12, 12),
):
    """Crop an image and add a legend."""
    img = Image.open(image_path).convert("RGBA")
    img = strip_frame_by_contrast(img)
    img = crop_white_margins(img, threshold=white_threshold, padding=white_padding)
    if legend_path:
        img = paste_legend(img, legend_path, pos=legend_pos, margin=legend_margin)

    img.save(out_path)


png_dir = Path(__file__).parents[2].joinpath(r"docs/png")
images = [i for i in png_dir.joinpath("qgis_export").glob("*.png") if i.stem != "legend"]
legend_path = png_dir.joinpath(r"qgis_export/legend.png")

for image_path in images:
    out_path = png_dir.joinpath(image_path.name)
    legend_pos = legend_positions[image_path.stem] if image_path.stem in legend_positions.keys() else "rt"
    crop_and_add_legend(
        image_path=image_path,
        out_path=out_path,
        legend_path=legend_path,
        legend_pos=legend_pos,
        white_padding=100,
        legend_margin=(50, 50),
        white_threshold=50,
    )
