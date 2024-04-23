"""
URL configuration for barcodeproject project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from barcodeapp import views
from django.contrib import admin

urlpatterns = [
    path('', views.home, name='home'),  # Add this line for the root URL
    path('admin/', admin.site.urls),
    path('api/receive/', views.receive_data, name='receive_data'),
    path('display/', views.display_page, name='display_page'),
    path('api/barcode/', views.barcode_api, name='barcode_api'),
]
