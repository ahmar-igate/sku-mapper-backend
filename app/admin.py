# app/admin.py
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.translation import gettext_lazy as _
from .models import User, new_product_mapping, product_mapping
admin.site.site_header = 'Sku Mapper Admin'
admin.site.site_title = 'Sku Mapper Admin Portal'
admin.site.index_title = 'Welcome to Sku Mapper Administration'

@admin.register(User)
class UserAdmin(BaseUserAdmin):
    ordering = ['email']
    list_display = ['email', 'first_name', 'last_name', 'is_staff', 'is_active', 'department']
    list_filter = ['is_staff', 'is_superuser', 'is_active', 'groups', 'department']
    
    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        (_('Personal info'), {'fields': ('first_name', 'last_name', 'department')}),
        (
            _('Permissions'),
            {'fields': ('is_active', 'is_staff', 'is_superuser', 'groups', 'user_permissions')}
        ),
        (_('Important dates'), {'fields': ('last_login', 'date_joined')}),
    )
    
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('email', 'password1', 'password2', 'department'),
        }),
    )

@admin.register(product_mapping)
class ProductMappingAdmin(admin.ModelAdmin):
    # Columns to display in the list view
    list_display = (
        'id', 'marketplace_sku', 'asin', 'im_sku', 'parent_sku',
        'region', 'sales_channel', 'level_1', 
        'linworks_title', 'modified_by', 'created_at', 'updated_at'
    )
    # Fields that can be searched
    search_fields = ('marketplace_sku', 'asin', 'im_sku', 'region','parent_sku', 'sales_channel', 'level_1')
    # Filter options in the sidebar
    list_filter = ('region', 'sales_channel', 'created_at')
    # Provides a drill-down navigation by date
    date_hierarchy = 'created_at'
    # Default ordering of results (most recent first)
    ordering = ('-created_at',)
    # Optional: set the number of records to display per page
    list_per_page = 25

@admin.register(new_product_mapping)
class NewProductMappingAdmin(admin.ModelAdmin):
    list_display = (
        'id', 'marketplace_sku', 'asin', 'im_sku', 
        'region', 'marketplace', 'level_1', 'level_2', 
        'level_3', 'level_4', 'level_5',  'parent_sku',
        'marketplace_sales_table', 'channel', 'company', 
        'created_at', 'updated_at'
    )
    search_fields = ('marketplace_sku', 'asin', 'im_sku', 'parent_sku', 'region', 'marketplace', 'company')
    list_filter = ('region', 'marketplace', 'channel', 'company', 'created_at')
    date_hierarchy = 'created_at'
    ordering = ('-created_at',)
    list_per_page = 25
