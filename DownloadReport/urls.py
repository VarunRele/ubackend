from django.urls import path
from . import views

urlpatterns = [
    path('run_report/<str:report_name>/', views.DownloadReport.as_view())
]