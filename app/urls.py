# app/urls.py
from django.urls import path
from rest_framework_simplejwt.views import TokenRefreshView
from app import views

urlpatterns = [
    path("dashboard/", views.Dashboard.as_view(), name="dashboard"),
    path("new_mapping/", views.New_Mapping.as_view(), name="new_mapping"),
    path("dump/", views.import_product_mapping, name="dump"),
    path('token/', views.CustomTokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path("update_mapping/<int:id>", views.UpdateMapping.as_view(), name="update_mapping"),
    path("save_mapping/", views.SaveMapping.as_view(), name="save_mapping"),
    
    # path('test-db/', views.test_db_connection, name='test_db_connection'),
]
