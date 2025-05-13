from django.core.management.base import BaseCommand, CommandError
from app.models import User

class Command(BaseCommand):
    help = 'Set department for a user by email'

    def add_arguments(self, parser):
        parser.add_argument('email', type=str, help='User email')
        parser.add_argument('department', type=str, help='Department (SCM, FINANCE, or ADMIN)')

    def handle(self, *args, **options):
        email = options['email']
        department = options['department']
        
        # Validate department
        if department not in [User.Department.SCM, User.Department.FINANCE, User.Department.ADMIN]:
            raise CommandError(f'Department must be one of: {User.Department.SCM}, {User.Department.FINANCE}, {User.Department.ADMIN}')
        
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist:
            raise CommandError(f'User with email {email} does not exist')
        
        user.department = department
        user.save()
        
        self.stdout.write(self.style.SUCCESS(f'Successfully set department for user {email} to {department}')) 