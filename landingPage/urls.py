from django.urls import path
from . import views

urlpatterns = [
    path("landing_page/<str:report_name>/", views.LandingPage.as_view())
]