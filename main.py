import converter

if __name__ == "__main__":
    converter.run_converter(
        "mapfiles/cyprus.osm.pbf",
        "vector_tiles.mbtiles",
        "render_tags.yaml"
    )
