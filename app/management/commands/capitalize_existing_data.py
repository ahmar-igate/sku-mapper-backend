from django.core.management.base import BaseCommand
from django.db import transaction
from app.models import product_mapping, new_product_mapping


class Command(BaseCommand):
    help = 'Capitalize existing data in product_mapping and new_product_mapping tables'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without making changes',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of records to process in each batch (default: 1000)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN MODE - No changes will be made'))
        
        self.stdout.write(self.style.SUCCESS('Starting capitalization process...'))
        
        # Process product_mapping table
        self.process_product_mapping(dry_run, batch_size)
        
        # Process new_product_mapping table
        self.process_new_product_mapping(dry_run, batch_size)
        
        self.stdout.write(self.style.SUCCESS('✅ Capitalization process completed!'))

    def process_product_mapping(self, dry_run, batch_size):
        self.stdout.write('\n' + '='*60)
        self.stdout.write(self.style.NOTICE('Processing product_mapping table...'))
        self.stdout.write('='*60)
        
        total_count = product_mapping.objects.using('default').count()
        self.stdout.write(f'Total records: {total_count}')
        
        updated_count = 0
        skipped_count = 0
        
        # Process in batches
        for offset in range(0, total_count, batch_size):
            records = product_mapping.objects.using('default').all()[offset:offset+batch_size]
            batch_updated = 0
            
            for record in records:
                changed = False
                changes = []
                
                # Check and capitalize marketplace_sku
                if record.marketplace_sku and str(record.marketplace_sku).strip():
                    new_value = record.marketplace_sku.strip().upper()
                    if record.marketplace_sku != new_value:
                        changes.append(f"marketplace_sku: '{record.marketplace_sku}' -> '{new_value}'")
                        record.marketplace_sku = new_value
                        changed = True
                
                # Check and capitalize asin
                if record.asin and str(record.asin).strip():
                    new_value = record.asin.strip().upper()
                    if record.asin != new_value:
                        changes.append(f"asin: '{record.asin}' -> '{new_value}'")
                        record.asin = new_value
                        changed = True
                
                # Check and capitalize im_sku
                if record.im_sku and str(record.im_sku).strip():
                    new_value = record.im_sku.strip().upper()
                    if record.im_sku != new_value:
                        changes.append(f"im_sku: '{record.im_sku}' -> '{new_value}'")
                        record.im_sku = new_value
                        changed = True
                
                # Check and capitalize parent_sku
                if record.parent_sku and str(record.parent_sku).strip():
                    new_value = record.parent_sku.strip().upper()
                    if record.parent_sku != new_value:
                        changes.append(f"parent_sku: '{record.parent_sku}' -> '{new_value}'")
                        record.parent_sku = new_value
                        changed = True
                
                # Check and capitalize region
                if record.region and str(record.region).strip():
                    new_value = record.region.strip().upper()
                    if record.region != new_value:
                        changes.append(f"region: '{record.region}' -> '{new_value}'")
                        record.region = new_value
                        changed = True
                
                # Check and capitalize level_1
                if record.level_1 and str(record.level_1).strip():
                    new_value = record.level_1.strip().upper()
                    if record.level_1 != new_value:
                        changes.append(f"level_1: '{record.level_1}' -> '{new_value}'")
                        record.level_1 = new_value
                        changed = True
                
                # Check and capitalize sales_channel (title case)
                if record.sales_channel and str(record.sales_channel).strip():
                    new_value = record.sales_channel.strip().capitalize()
                    if record.sales_channel != new_value:
                        changes.append(f"sales_channel: '{record.sales_channel}' -> '{new_value}'")
                        record.sales_channel = new_value
                        changed = True
                
                if changed:
                    if not dry_run:
                        # Use update_fields to avoid triggering save() method which would apply the same logic
                        record.save(update_fields=['marketplace_sku', 'asin', 'im_sku', 'parent_sku', 'region', 'level_1', 'sales_channel'])
                    updated_count += 1
                    batch_updated += 1
                    
                    if dry_run and updated_count <= 10:  # Show first 10 examples in dry run
                        self.stdout.write(f"  Record ID {record.id}: {', '.join(changes)}")
                else:
                    skipped_count += 1
            
            # Show progress
            processed = min(offset + batch_size, total_count)
            self.stdout.write(f'Processed {processed}/{total_count} records ({batch_updated} updated in this batch)...')
        
        self.stdout.write(self.style.SUCCESS(f'\nproduct_mapping summary:'))
        self.stdout.write(f'  - Updated: {updated_count}')
        self.stdout.write(f'  - Skipped (no changes): {skipped_count}')

    def process_new_product_mapping(self, dry_run, batch_size):
        self.stdout.write('\n' + '='*60)
        self.stdout.write(self.style.NOTICE('Processing new_product_mapping table...'))
        self.stdout.write('='*60)
        
        total_count = new_product_mapping.objects.using('default').count()
        self.stdout.write(f'Total records: {total_count}')
        
        if total_count == 0:
            self.stdout.write('  No records found in new_product_mapping table.')
            return
        
        updated_count = 0
        skipped_count = 0
        
        # Process in batches
        for offset in range(0, total_count, batch_size):
            records = new_product_mapping.objects.using('default').all()[offset:offset+batch_size]
            batch_updated = 0
            
            for record in records:
                changed = False
                changes = []
                
                # Check and capitalize marketplace_sku
                if record.marketplace_sku and str(record.marketplace_sku).strip():
                    new_value = record.marketplace_sku.strip().upper()
                    if record.marketplace_sku != new_value:
                        changes.append(f"marketplace_sku: '{record.marketplace_sku}' -> '{new_value}'")
                        record.marketplace_sku = new_value
                        changed = True
                
                # Check and capitalize asin
                if record.asin and str(record.asin).strip():
                    new_value = record.asin.strip().upper()
                    if record.asin != new_value:
                        changes.append(f"asin: '{record.asin}' -> '{new_value}'")
                        record.asin = new_value
                        changed = True
                
                # Check and capitalize im_sku
                if record.im_sku and str(record.im_sku).strip():
                    new_value = record.im_sku.strip().upper()
                    if record.im_sku != new_value:
                        changes.append(f"im_sku: '{record.im_sku}' -> '{new_value}'")
                        record.im_sku = new_value
                        changed = True
                
                # Check and capitalize parent_sku
                if record.parent_sku and str(record.parent_sku).strip():
                    new_value = record.parent_sku.strip().upper()
                    if record.parent_sku != new_value:
                        changes.append(f"parent_sku: '{record.parent_sku}' -> '{new_value}'")
                        record.parent_sku = new_value
                        changed = True
                
                # Check and capitalize region
                if record.region and str(record.region).strip():
                    new_value = record.region.strip().upper()
                    if record.region != new_value:
                        changes.append(f"region: '{record.region}' -> '{new_value}'")
                        record.region = new_value
                        changed = True
                
                # Check and capitalize level_1
                if record.level_1 and str(record.level_1).strip():
                    new_value = record.level_1.strip().upper()
                    if record.level_1 != new_value:
                        changes.append(f"level_1: '{record.level_1}' -> '{new_value}'")
                        record.level_1 = new_value
                        changed = True
                
                if changed:
                    if not dry_run:
                        # Use update_fields to avoid triggering save() method
                        record.save(update_fields=['marketplace_sku', 'asin', 'im_sku', 'parent_sku', 'region', 'level_1'])
                    updated_count += 1
                    batch_updated += 1
                    
                    if dry_run and updated_count <= 10:  # Show first 10 examples in dry run
                        self.stdout.write(f"  Record ID {record.id}: {', '.join(changes)}")
                else:
                    skipped_count += 1
            
            # Show progress
            processed = min(offset + batch_size, total_count)
            self.stdout.write(f'Processed {processed}/{total_count} records ({batch_updated} updated in this batch)...')
        
        self.stdout.write(self.style.SUCCESS(f'\nnew_product_mapping summary:'))
        self.stdout.write(f'  - Updated: {updated_count}')
        self.stdout.write(f'  - Skipped (no changes): {skipped_count}')
