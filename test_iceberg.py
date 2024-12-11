from pyiceberg.catalog import load_catalog

def test_iceberg_catalog():
    try:
        # Load the catalog using the name defined in .pyiceberg.yaml
        catalog = load_catalog("rest")  # Replace "rest" with your catalog name
        print("Catalog loaded successfully.")

        # List namespaces
        namespaces = catalog.list_namespaces()
        print("Namespaces available in the catalog:", namespaces)

        # For each namespace, list tables
        for namespace in namespaces:
            print(f"Namespace: {namespace}")
            tables = catalog.list_tables(namespace=namespace)
            print(f"Tables in namespace '{namespace}': {tables}")

    except Exception as e:
        print("Error occurred while testing the Iceberg catalog:", e)

if __name__ == "__main__":
    test_iceberg_catalog()
