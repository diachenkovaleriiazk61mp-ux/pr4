from datetime import date
import unicodedata

FIELDS = ('patient_code', 'vaccine', 'batch', 'dose_number', 'administration_date', 'facility')

def validate(data):
    errors = {}
    if not isinstance(data, dict):
        return {'body': 'Очікується JSON об’єкт'}
    for key in data.keys() - set(FIELDS):
        errors[key] = 'Невідоме поле; id генерує база'
    for key in FIELDS:
        if key not in data:
            errors[key] = 'Обов’язкове поле'
    for key in ('patient_code', 'vaccine', 'batch', 'facility'):
        if key in data and (not isinstance(data[key], str) or not data[key].strip() or len(data[key]) > 200):
            errors[key] = 'Непорожній рядок до 200 символів'
    for key in ('patient_code', 'vaccine', 'batch', 'facility'):
        if isinstance(data.get(key), str) and any(unicodedata.category(c) == 'Cc' for c in data[key]):
            errors[key] = 'Керівні символи заборонено'
    if 'dose_number' in data and (type(data['dose_number']) is not int or not 1 <= data['dose_number'] <= 4):
        errors['dose_number'] = 'Ціле число від 1 до 4'
    if 'administration_date' in data:
        try:
            value = data['administration_date']
            if not isinstance(value, str) or len(value) != 10:
                raise ValueError()
            parsed = date.fromisoformat(value)
            if parsed > date.today():
                errors['administration_date'] = 'Дата введення не може бути в майбутньому'
        except (TypeError, ValueError):
            errors['administration_date'] = 'Дата у форматі YYYY-MM-DD'
    return errors

