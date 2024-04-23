from rest_framework import serializers

class BarcodeSerializer(serializers.Serializer):
    barcode = serializers.CharField(max_length=200)
    timestamp = serializers.CharField(max_length=200)
