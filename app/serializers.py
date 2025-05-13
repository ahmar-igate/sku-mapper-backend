# app/serializers.py
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework import serializers
from app.models import product_mapping, new_product_mapping
class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    # Use email as the username field
    username_field = 'email'

    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        # Add custom claims
        token['email'] = user.email
        token['department'] = user.department
        return token
    
class ProductMappingSerializer(serializers.ModelSerializer):
    class Meta:
        model = product_mapping
        fields = '__all__'  
        
class NewProductMappingSerializer(serializers.ModelSerializer):
    class Meta:
        model = new_product_mapping
        fields = '__all__'  