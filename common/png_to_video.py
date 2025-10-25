"""
Stores code to convert a sequence of PNG images to a video file.
"""
from __future__ import annotations

# IMPORTs standard
import os
import shutil
import subprocess

# TYPE ANNOTATIONs
from typing import Literal
type Codec = Literal[
    "libx264", "libx265", "mpeg4", "libvpx-vp9", "h264_nvenc", "hevc_nvenc", "libxvid", "prores_ks"
]
type PixFmt = Literal["yuv420p","yuv422p","yuv444p","rgba"]

# API public
__all__ = ['PngToVideo']



class PngToVideo:
    """
    To create an mp4 video from pngs using ffmpeg.
    If on Windows OS, you might need to install ffmpeg.exe and add it to PATH prior to using this
    class.
    """

    def __init__(
            self,
            video_name: str,
            png_dir: str,
            output_dir: str,
            fps: int = 30,
            codec: Codec = "libx264",
            crf: int = 18,
            png_pattern: str = "frame_%05d.png",
            pix_fmt: PixFmt = "yuv420p",
            png_catalogue: str | None = None,
            extra_args: list[str] | None = None,
        ) -> None:
        """
        Creates an mp4 video from a sequence of png images.
        If on Windows OS, you might need to install ffmpeg.exe and add it to PATH prior to 
        initiating this class.

        Args:
            video_name (str): name of the output video file (with .mp4 extension).
            png_dir (str): directory where the png images are stored.
            output_dir (str): directory where the output video will be saved.
            fps (int, optional): frames per second of the output video. Defaults to 30.
            codec (str, optional): video codec to use. Defaults to "libx264".
            crf (int, optional): constant rate factor (quality) to use. The lower the value, the
                better the quality. Range is 0-51. Defaults to 18.
            png_pattern (str, optional): shell pattern to match the input PNG files. If
                'png_catalogue' is provided, this parameter is ignored.
                Defaults to "frame_%05d.png".
            pix_fmt (str, optional): pixel format of the output video. Defaults to "yuv420p".
            png_catalogue (str | None, optional): catalogue filename listing the ordered input PNG
                files. If provided,'png_pattern' is ignored. Defaults to None.
            extra_args (list[str] | None, optional): extra arguments to pass to ffmpeg command.
                Defaults to None.
        """

        # PARAMETERs
        self._crf = crf
        self._fps = fps
        self._codec = codec
        self._png_dir = png_dir
        self._pix_fmt = pix_fmt
        self._video_name = video_name
        self._output_dir = output_dir
        self._extra_args = extra_args if extra_args is not None else []
        self._png_pattern = png_pattern
        self._png_catalogue = png_catalogue

        # CHECKs
        self._checks()

        # RUN
        self._create_video()

    def _checks(self) -> None:
        """
        Does some checks and also creates the directory paths.

        Raises:
            FileNotFoundError: if the input png directory does not exist.
        """

        if shutil.which('ffmpeg') is None:
            raise FileNotFoundError(
                "FFmpeg executable not found. Install ffmpeg or add it to PATH before retrying."
            )

        if not os.path.exists(self._png_dir):
            raise FileNotFoundError(f"The PNG directory '{self._png_dir}' does not exist.")
        os.makedirs(self._output_dir, exist_ok=True)

        if (
            self._png_catalogue is not None
        ) and (
            not os.path.exists(os.path.join(self._png_dir, self._png_catalogue))
        ):
            raise FileNotFoundError(
                f"The PNG catalogue file '{self._png_catalogue}' does not exist."
            )

    def _create_video(self) -> None:
        """
        Creates the video from the png files using ffmpeg.
        The command is run using bash (with subprocess).
        """

        # INIT
        cmd = [
            'ffmpeg',
            '-framerate', str(self._fps),
        ]

        # INPUT
        if self._png_catalogue is not None:
            cmd += [
                '-f', 'concat',
                '-safe', '0',
                '-i', os.path.join(self._png_dir, self._png_catalogue),
            ]
        else:
            cmd += [
                '-f', 'image2',
                '-i', os.path.join(self._png_dir, self._png_pattern),
            ]

        # END cmd
        cmd += [
            '-c:v', f'{self._codec}',
            '-crf', str(self._crf),
            '-pix_fmt', f'{self._pix_fmt}',
            *self._extra_args,
            '-y',  # Overwrite output file
            os.path.join(self._output_dir, self._video_name),
        ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            print(f"Video created: {os.path.join(self._output_dir, self._video_name)}")
        except subprocess.CalledProcessError as e:
            print(f"FFmpeg error: {e.stderr.decode()}")



if __name__ == '__main__':
    PngToVideo(
        video_name='test_video.mp4',
        png_dir='/home/voyeux-alfred/Documents/work_codes/SOHO_30years_video/results/png',
        output_dir='/home/voyeux-alfred/Documents/work_codes/SOHO_30years_video/results/',
        fps=24,
        codec='libx264',
        crf=20,
        png_pattern='frame_%05d.png',
        pix_fmt='yuv420p',
        png_catalogue=None,
        # extra_args=['-preset', 'fast'],
    )
