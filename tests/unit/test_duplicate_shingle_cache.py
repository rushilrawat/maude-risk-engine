import maude.features.duplicates as duplicates


def test_unpacked_shingle_blobs_are_reused_with_a_bounded_cache() -> None:
    cache_clear = getattr(duplicates._unpack_shingles, "cache_clear", None)
    cache_info = getattr(duplicates._unpack_shingles, "cache_info", None)
    assert callable(cache_clear)
    assert callable(cache_info)

    cache_clear()
    packed = duplicates._pack_shingles((11, 22, 33))

    first = duplicates._unpack_shingles(packed)
    second = duplicates._unpack_shingles(packed)

    assert first == frozenset((11, 22, 33))
    assert first is second
    assert cache_info().hits == 1
    assert cache_info().maxsize == 4_096
