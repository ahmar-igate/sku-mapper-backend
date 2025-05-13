# app/models.py
from django.db import models
from django.contrib.auth.models import (
    AbstractBaseUser, PermissionsMixin, BaseUserManager
)
from django.utils import timezone


class UserManager(BaseUserManager):
    def create_user(self, email, password=None, **extra_fields):
        """
        Create and save a User with the given email and password.
        """
        if not email:
            raise ValueError('The Email field must be set')
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.set_password(password)  # Uses Django's secure hashing
        user.save(using=self._db)
        return user

    def create_superuser(self, email, password, **extra_fields):
        """
        Create and save a SuperUser with the given email and password.
        """
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_active', True)  # Ensure superusers are active

        if extra_fields.get('is_staff') is not True:
            raise ValueError('Superuser must have is_staff=True.')
        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True.')
        return self.create_user(email, password, **extra_fields)


class User(AbstractBaseUser, PermissionsMixin):
    class Department(models.TextChoices):
        SCM = 'SCM', 'Supply Chain Management'
        FINANCE = 'FINANCE', 'Finance Department'
        ADMIN = 'ADMIN', 'Administrator'
    
    email = models.EmailField(unique=True)
    # Optional additional fields
    first_name = models.CharField(max_length=30, blank=True)
    last_name = models.CharField(max_length=30, blank=True)
    department = models.CharField(
        max_length=10,
        choices=Department.choices,
        default=Department.SCM,
    )
    
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    date_joined = models.DateTimeField(default=timezone.now)

    # Set the unique identifier to email
    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = []  # Email & password are required by default

    objects = UserManager()

    def __str__(self):
        return self.email
    
    class Meta:
        db_table = 'sku_mapper_user'


class product_mapping(models.Model):
    id = models.AutoField(primary_key=True)
    date = models.CharField(max_length=255, null=True, blank=True)
    marketplace_sku = models.CharField(max_length=255)
    asin = models.CharField(max_length=255)
    im_sku = models.CharField(max_length=255, null=True, blank=True)
    region = models.CharField(max_length=255)
    sales_channel = models.CharField(max_length=255, null=True, blank=True)
    level_1 = models.CharField(max_length=255, null=True, blank=True)
    linworks_title = models.TextField(null=True, blank=True)
    amazon_title = models.TextField(null=True, blank=True)
    parent_sku = models.CharField(max_length=255, blank=True, null=True)
    modified_by = models.CharField(max_length=255, null=True, blank=True)
    modified_by_finance = models.CharField(max_length=255, null=True, blank=True)
    modified_by_admin = models.CharField(max_length=255, null=True, blank=True)
    comment = models.TextField(null=True, blank=True)
    comment_by_finance = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"MarketPlace SKU {self.marketplace_sku} in region {self.region}"
    
    class Meta:
        db_table = 'product_mapping'
        
class new_product_mapping(models.Model):
    id = models.AutoField(primary_key=True)
    marketplace_sku = models.CharField(max_length=255, unique=True)
    asin = models.CharField(max_length=255)
    im_sku = models.CharField(max_length=255, null=True, blank=True)
    parent_sku = models.CharField(max_length=255, blank=True, null=True)
    region = models.CharField(max_length=255)
    marketplace = models.CharField(max_length=255)
    level_1 = models.CharField(max_length=255, null=True, blank=True)
    level_2 = models.CharField(max_length=255, null=True, blank=True)
    linworks_title = models.TextField(null=True, blank=True)
    level_3 = models.CharField(max_length=255, null=True, blank=True)
    level_4 = models.CharField(max_length=255, null=True, blank=True)  
    level_5 = models.CharField(max_length=255, null=True, blank=True)
    marketplace_sales_table = models.CharField(max_length=255, default='stg_tr_amazon_raw')
    channel = models.CharField(max_length=255, default='Amazon')
    company = models.CharField(max_length=255)
    modified_by = models.CharField(max_length=255, null=True, blank=True)
    modified_by_finance = models.CharField(max_length=255, null=True, blank=True)
    modified_by_admin = models.CharField(max_length=255, null=True, blank=True)
    comment = models.TextField(null=True, blank=True)
    comment_by_finance = models.TextField(null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"MarketPlace SKU {self.marketplace_sku} in region {self.region}"
    
    class Meta:
        db_table = 'new_product_mapping'