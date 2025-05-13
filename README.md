# SKU Mapper Backend

SKU Mapper is a Django-based dashboard application that displays data from your database and allows users to map SKUs. Built on Django REST Framework, it provides a simple yet powerful interface for managing and visualizing SKU data.

## Features

- **RESTful API**: Exposes endpoints for seamless integration and data manipulation.
- **Built with Modern Tools**: Utilizes Django 4.2.19 and Python 3.9.0 for a robust development experience.

## Prerequisites

- **Python**: 3.9.0
- **Django**: 4.2.19
- **Django REST Framework**: 3.15.2

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/ahmar-igate/sku-mapper-b.git
   cd sku-mapper-b

2. **Create and Activate a Virtual Environment**

    ```bash
    python3 -m venv venv
    source venv/bin/activate   # For Windows use: venv\Scripts\activate

3. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
    
4. **Environment Configuration**
   
   Create a .env file in the root directory and add your environment-specific settings:

   ```bash
   SECONDARY_ENGINE=
   SECONDARY_NAME=
   SECNDARY_USER=
   SECONDARY_PASSWORD=
   SECONDARY_HOST=
   
   TERTIARY_ENGINE=
   TERTIARY_NAME=
   TERTIARY_USER=
   TERTIARY_PASSWORD=
   TERTIARY_HOST=
   
   DJANGO_SECRET_KEY=

5. **Apply Migrations**
   
   Run the following command to set up your database:

   ```bash
   python manage.py migrate --database=default

6. **Start the Development Server**

   Start the development server with:

   ```bash
   python manage.py runserver

## Usage
- **Dashboard**: Access the dashboard by navigating to your frontend server (default: http://127.0.0.1:5173) in your browser. Here, you can view all database entries.
- **SKU Mapping**: Use the provided interface to map SKUs according to your business logic. You can setup your frontend by following this [repository](https://github.com/ahmar-igate/shopify-connector-f.git).

## API Endpoints

  The application provides several RESTful endpoints. Some key endpoints include:

  - `/dashboard` – Endpoint to list, create, update, or delete SKU mappings.
  - `/new_mapping` – Endpoint to retrieve the records from secondary db.
  - `/dump` - Endpoint to save new data to database.
  - `/update_mapping/<int:id>` - Endpoint to update the mapping record to default db.
  - `/save_mapping` - Endpoint to save mapped records to teriary db

  Feel free to explore these endpoints using tools like Postman or cURL.

## Contact

For questions or support, please reach out at ahmaraamir33@gmail.com.
   
