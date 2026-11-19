"""Render the platform pairing URL locally."""

import base64
from io import BytesIO

import qrcode
from qrcode.exceptions import DataOverflowError
from qrcode.image.svg import SvgPathImage


def qr_image(content: str) -> str:
    if not isinstance(content, str) or not 0 < len(content) <= 4096:
        raise ValueError("Invalid QR content")
    try:
        image = qrcode.make(content, image_factory=SvgPathImage)
    except DataOverflowError:
        raise ValueError("Invalid QR content") from None
    buffer = BytesIO()
    image.save(buffer)
    return "data:image/svg+xml;base64," + base64.b64encode(buffer.getvalue()).decode()
