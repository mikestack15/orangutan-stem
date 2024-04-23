from rest_framework.decorators import api_view
from rest_framework.response import Response
from .serializers import BarcodeSerializer
from django.http import HttpResponse
import os
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render


data_storage = {'barcode': 'No barcode yet', 'timestamp': 'No timestamp yet'}

@api_view(['GET', 'POST'])
def barcode_api(request):
    if request.method == 'POST':
        serializer = BarcodeSerializer(data=request.data)
        if serializer.is_valid():
            data_storage['barcode'] = serializer.validated_data['barcode']
            data_storage['timestamp'] = serializer.validated_data['timestamp']
            return Response({'status': 'success', 'data': serializer.data})
        else:
            return Response(serializer.errors, status=400)
    elif request.method == 'GET':
        return Response(data_storage)


def barcode_display(request):
    html_file_path = os.path.join(settings.STATIC_ROOT, 'html', 'barcode_display.html')
    with open(html_file_path, 'r') as file:
        return HttpResponse(file.read(), content_type="text/html")


@csrf_exempt
def receive_data(request):
    if request.method == 'POST':
        # Your code for handling POST requests
        return JsonResponse({'status': 'success'})
    else:
        return JsonResponse({'status': 'error', 'message': 'Invalid request'}, status=400)


def display_page(request):
    # your code to handle the request
    return render(request, 'your_template_name.html')

def home(request):
    return render(request, 'home.html')


def home(request):
    return render(request, 'index.html')
