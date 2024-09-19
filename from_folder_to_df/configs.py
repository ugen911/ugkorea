# Конфигурация для каждого датафрейма
configs = {
    'berg': {
        'rename_columns': {
            'Артикул': 'артикул', 'Наименование': 'наименование',
            'Бренд': 'производитель', 'Склад': 'склад',
            'Количество': 'количество', 'Цена руб': 'цена'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'склад', 'количество', 'цена'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'цена': [(',', '.')]}
    },
    'shateekat': {
        'rename_columns': {
            'Бренд': 'производитель', 'Каталожный номер': 'артикул',
            'Описание': 'наименование', 'Остаток': 'количество', 'Цена': 'цена'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'количество', 'цена'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'количество': [('>', '')]}
    },
    'shatepodolsk': {
        'rename_columns': {
            'Бренд': 'производитель', 'Каталожный номер': 'артикул',
            'Описание': 'наименование', 'Остаток': 'количество', 'Цена': 'цена'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'количество', 'цена'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'количество': [('>', '')]}
    },
    'favorit': {
        'rename_columns': {
            'Производитель': 'производитель', 'Номер по каталогу': 'артикул',
            'Наименование': 'наименование', 'Цена по договору': 'цена', 'Количество': 'количество'
        },
        'column_order': ['артикул', 'наименование', 'производитель', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },
    'forumcenter': {
        'rename_columns': {
            'ГРУППА': 'производитель', '№ ПРОИЗВ.': 'артикул',
            'НАИМЕНОВАНИЕ': 'наименование', 'ЦЕНА, РУБ': 'цена', 'НАЛичие': 'количество'
        },
        'column_order': ['производитель', 'артикул', 'наименование', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },
    'forumnvs': {
        'rename_columns': {
            'ГРУППА': 'производитель', '№ ПРОИЗВ.': 'артикул',
            'НАИМЕНОВАНИЕ': 'наименование', 'ЦЕНА, РУБ': 'цена', 'НАЛичие': 'количество'
        },
        'column_order': ['производитель', 'артикул', 'наименование', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },

    'forumkrsk': {
        'rename_columns': {
            'ГРУППА': 'производитель', '№ ПРОИЗВ.': 'артикул',
            'НАИМЕНОВАНИЕ': 'наименование', 'ЦЕНА, РУБ': 'цена', 'НАЛичие': 'количество'
        },
        'column_order': ['производитель', 'артикул', 'наименование', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int}
    },

    'tiss': {
        'rename_columns': {
            'Бренд': 'производитель', 'Наименование товаров': 'наименование',
            'Катал. номер': 'артикул', 'ОПТ': 'цена', 'Кол-во всего': 'количество'
        },
        'column_order': ['производитель', 'наименование', 'артикул', 'цена', 'количество'],
        'convert_types': {'цена': float, 'количество': int},
        'replace_values': {'цена': [(',', '.')]}
    }
}