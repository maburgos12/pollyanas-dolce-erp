from rest_framework.pagination import PageNumberPagination


class StandardPagination(PageNumberPagination):
    page_size = 50
    page_size_query_param = "page_size"
    max_page_size = 500


class LargePagination(PageNumberPagination):
    page_size = 200
    page_size_query_param = "page_size"
    max_page_size = 2000
