database_dir = "dbfiles/"

allowed_tags = {
    # Классификация
    'highway', 'building', 'natural', 'landuse', 'waterway',
    'amenity', 'shop', 'tourism', 'leisure', 'railway',

    # Названия и текст
    'name', 'name:ru', 'name:en',  # можно оставить локализованные имена

    # Свойства дорог
    'oneway', 'surface', 'lanes', 'bridge', 'tunnel', 'maxspeed',

    # Адреса (опционально, весят много)
    'addr:housenumber', 'addr:street',

    # Границы
    'boundary', 'admin_level', 'place'
}


ZOOM_LEVELS = [1, 2, 5, 8, 11, 13, 14, 15, 16, 17, 18]