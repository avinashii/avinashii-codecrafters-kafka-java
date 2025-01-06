package io;

import java.io.IOException;

public class VarInt {
	
	public static long readLong(DataInput input) throws IOException{
		
		long tmp;
		
		if((tmp = input.readSignedByte()) >= 0) {
			return tmp;
		}
		
		long result = tmp & 0x7f;
		
		if( (tmp = input.readSignedByte()) >= 0) {
			result |= tmp<<7;
		}else {
			result |= (tmp & 0x7f) << 7;
			if((tmp = input.readSignedByte()) >= 0) {
				result |= tmp<<14;
			}else {
				result |= (tmp& 0x7f) << 14;
				if((tmp = input.readSignedLong()) >=0) {
					result |= tmp<<21;
				}else {
					result |= (tmp & 0x7f) << 21;
					if( (tmp = input.readSignedLong()) >= 0) {
						result |= tmp<<28;
					}else {
						result |= (tmp & 0x7f) << 28;
						if((tmp = input.readSignedLong()) >=0) {
							result |= tmp<<35;
						}else {
							result |= (tmp & 0x7f) << 35;
							if( (tmp = input.readSignedLong()) >= 0) {
								result |= tmp <<42;
							}else {
								result |= (tmp & 0x7f) << 42;
								if( (tmp = input.readSignedLong()) >= 0) {
									result |= tmp<<49;
								}else {
									result |= (tmp & 0x7f) << 49;
									if( (tmp = input.readSignedLong()) >= 0) {
										result |= tmp<<56;
									}else {
										result |= (tmp& 0x7f) <<56;
										result |= ((long) input.readSignedLong() << 63);
									}
								}
							}
						}
					}
				}
			}
		}
		
		
		return result;
	}
	
	public static void writeLong(long value , DataOutput output) throws IOException{
		
		while(true) {
			int bits = ((int) value) & 0x7f;
			
			value >>= 7;
			if(value == 0) {
				output.writeByte((byte) bits);
			}
			
			output.writeByte((byte) (bits|(0x80)));
		}
	}
 
}
