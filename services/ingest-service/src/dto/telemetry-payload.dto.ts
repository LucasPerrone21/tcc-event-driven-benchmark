import { IsISO8601, IsNotEmpty, IsNumber, IsOptional, IsString } from 'class-validator';

export class TelemetryPayloadDto {
  @IsOptional()
  @IsString()
  @IsNotEmpty()
  message_id?: string;

  @IsString()
  @IsNotEmpty()
  sensor_id!: string;

  @IsString()
  @IsNotEmpty()
  type!: string;

  @IsNumber({ allowNaN: false, allowInfinity: false })
  value!: number;

  @IsString()
  @IsNotEmpty()
  unit!: string;

  @IsOptional()
  @IsISO8601()
  sensor_created_at?: string;

  @IsOptional()
  @IsISO8601()
  timestamp?: string;
}
