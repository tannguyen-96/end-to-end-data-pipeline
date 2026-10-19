select 
    user_id::number as customer_id, 
    name as customer_name, 
    lower(email) as email,
    age,
    gender, 
    marital_status, 
    occupation,
    monthly_income as income_band, 
    educational_qualifications, 
    family_size
from {{ ref('users')}}
where is_deleted = false