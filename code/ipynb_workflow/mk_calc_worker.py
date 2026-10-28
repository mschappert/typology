"""Mann-Kendall Tau tile worker for the time-series notebook.

Defined as a real importable module so it works with the ``spawn`` start
method used by Jupyter on Windows and in Docker, without embedding source in
a string or loading it from a temporary directory.

Each worker reads its own spatial window from every annual raster, so only
the tile position, window offsets, and input paths cross the process
boundary. Open datasets are cached per process and reused across tiles.
"""

import os

import numpy as np
import rasterio
from rasterio.windows import Window

# Per-process cache of open rasterio datasets, keyed by the input path tuple.
_SOURCE_CACHE = {}


def _init_worker_gdal_env(gdal_cache_mb):
    """Pool initializer: cap GDAL's block cache in each spawn worker.

    Spawn workers start as fresh interpreters and inherit none of the parent's
    GDAL configuration. Setting GDAL_CACHEMAX here (before any dataset is
    opened in the worker) keeps every worker's decompressed-block cache bounded
    so the combined memory of all workers stays within the job's RAM budget.
    """
    os.environ["GDAL_CACHEMAX"] = str(int(gdal_cache_mb))


def _get_sources(input_paths):
    sources = _SOURCE_CACHE.get(input_paths)
    if sources is None:
        sources = [rasterio.open(path) for path in input_paths]
        _SOURCE_CACHE[input_paths] = sources
    return sources


def calculate_mk_for_tile(tile_data):
    """Calculate Tau = S / [n(n-1)/2] for eligible pixels in one tile.

    Uses the sign-sum identity S = sum_{i<j} sign(x_j - x_i). Equal-valued
    pairs contribute 0. Missing observations are omitted pairwise: a NaN in
    either member makes that pair's difference NaN, which ``np.nansum`` skips.
    Pixels below ``min_observations`` valid years are returned as NaN.

    The year loop runs only over earlier years (O(years), not O(years^2) in
    Python); each step compares one earlier year against all later years at
    once, so the heavy work stays in vectorized NumPy C loops.
    """
    row, col, window_offsets, input_paths, min_observations = tile_data
    col_off, row_off, block_width, block_height = window_offsets
    window = Window(col_off, row_off, block_width, block_height)
    sources = _get_sources(input_paths)
    n_years = len(sources)

    yearly = np.empty((n_years, block_height, block_width), dtype=np.float32)
    for year_index, source in enumerate(sources):
        values = source.read(1, window=window).astype(np.float32)
        valid = (source.read_masks(1, window=window) > 0) & np.isfinite(values)
        yearly[year_index] = np.where(valid, values, np.nan)

    # Background removal using the time series itself: a pixel that is 0 (or
    # missing) in EVERY year is background outside the study area and is
    # dropped. A pixel with any nonzero value in any year is kept in full,
    # including its genuine 0s -- a value rising from 0 is a real increase and
    # a value falling to 0 is a real decrease, so those 0s must stay in the
    # trend. Dropped pixels become all-NaN and fail the min_observations check
    # below, so their Tau is NaN (blank), not a misleading 0.
    background = np.all(np.isnan(yearly) | (yearly == 0), axis=0)
    yearly[:, background] = np.nan

    # Endpoint rule: require the first OR last year to have a real (non-zero,
    # non-missing) value. If BOTH endpoints are 0/missing, the pixel is treated
    # as background and dropped (set to all-NaN -> NoData). This removes
    # transient "blip" pixels that were absent at both 1990 and 2020 but briefly
    # held forest in between (appear-then-disappear), which have no sustained
    # trend and should not be classified.
    first_ok = np.isfinite(yearly[0]) & (yearly[0] != 0)
    last_ok = np.isfinite(yearly[-1]) & (yearly[-1] != 0)
    endpoint_background = ~(first_ok | last_ok)
    yearly[:, endpoint_background] = np.nan

    pixels = yearly.reshape(n_years, block_height * block_width)
    observation_count = np.isfinite(pixels).sum(axis=0)
    eligible_indices = np.flatnonzero(observation_count >= min_observations)
    tau = np.full(observation_count.size, np.nan, dtype=np.float32)

    if eligible_indices.size:
        selected = pixels[:, eligible_indices].astype(np.float32, copy=False)
        selected_count = observation_count[eligible_indices].astype(np.float64)
        s_statistic = np.zeros(eligible_indices.size, dtype=np.float64)
        difference = np.empty_like(selected)
        for earlier_year in range(n_years - 1):
            later = selected[earlier_year + 1:]
            # sign(later - earlier) for every later year at once; NaN pairs
            # stay NaN and are dropped by nansum below.
            block = difference[earlier_year + 1:]
            np.subtract(later, selected[earlier_year], out=block)
            np.sign(block, out=block)
            s_statistic += np.nansum(block, axis=0, dtype=np.float64)
        pair_count = selected_count * (selected_count - 1.0) / 2.0
        tau[eligible_indices] = (s_statistic / pair_count).astype(np.float32)

    return row, col, tau.reshape(block_height, block_width)
